use crate::config::Config;
use crate::errors::CallystoError::GeneralError;
use crate::errors::*;
use crate::kafka::cconsumer::CConsumer;
use crate::kafka::contexts::{CConsumerContext, CStatistics};
use crate::kafka::ctopic::{CTopic, CTP};
use crate::kafka::enums::ProcessingGuarantee;
use crate::stores::inmemory::InMemoryStore;
use crate::stores::store::Store;
use crate::types::collection::Collection;
use crate::types::context::Context;
use crate::types::service::{Service, ServiceState};
use crate::types::table_agent::TableAgent;
use async_trait::async_trait;
use crossbeam_channel::{unbounded, Receiver, Sender};
use futures::future::BoxFuture;
use futures::{FutureExt, SinkExt};
use futures_timer::Delay;
use lever::sync::atomics::AtomicBox;
use lightproc::prelude::State;
use rdkafka::message::OwnedMessage;
use rdkafka::{ClientConfig, ClientContext, Message};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};
use tracing_subscriber::filter::FilterExt;
use url::Url;

#[derive(Clone)]
pub struct CTable<State = ()>
where
    State: Clone + Send + Sync + 'static,
{
    pub app_name: String,
    pub table_name: String,
    storage_url: Url,
    config: Config,
    pub source_topic_consumer_context: Arc<AtomicBox<Option<CConsumerContext>>>,
    pub changelog_topic: CTopic,
    pub changelog_tx: Sender<(usize, Vec<u8>, Vec<u8>)>,
    pub changelog_rx: Receiver<(usize, Vec<u8>, Vec<u8>)>,
    data: Arc<dyn Store<State>>,
}

impl<State> CTable<State>
where
    State: Clone + Send + Sync + 'static,
{
    pub fn new(
        app_name: String,
        storage_url: Url,
        table_name: String,
        config: Config,
        client_config: ClientConfig,
    ) -> Result<Self> {
        let data = Self::new_storage(app_name.clone(), storage_url.clone(), table_name.clone())?;
        let changelog_topic = CTopic::new(
            format!("{}-{}-changelog", app_name, table_name),
            client_config,
        );

        let (changelog_tx, changelog_rx) = unbounded::<(usize, Vec<u8>, Vec<u8>)>();

        Ok(Self {
            app_name,
            table_name,
            storage_url,
            config,
            source_topic_consumer_context: Arc::new(AtomicBox::new(None)),
            changelog_topic,
            changelog_tx,
            changelog_rx,
            data,
        })
    }

    ///
    /// Get element on the table
    pub fn get<K, V>(&self, key: K, msg: OwnedMessage) -> Result<Option<V>>
    where
        K: Serialize,
        V: DeserializeOwned,
    {
        let serialized_key = bincode::serialize(&key)?;
        match <Self as Store<State>>::get(self, serialized_key, msg)? {
            Some(value_slice) => Ok(Some(bincode::deserialize::<V>(value_slice.as_slice())?)),
            _ => Ok(None),
        }
    }

    ///
    /// Set element on the table
    pub fn set<K, V>(&self, key: K, value: V, msg: OwnedMessage) -> Result<()>
    where
        K: Serialize,
        V: Serialize,
    {
        let serialized_key = bincode::serialize(&key)?;
        let serialized_val = bincode::serialize(&value)?;
        <Self as Store<State>>::set(self, serialized_key, serialized_val, msg)
    }

    ///
    /// Delete element on the table
    pub fn del<K>(&self, key: K, msg: OwnedMessage) -> Result<()>
    where
        K: Serialize,
    {
        let serialized_key = bincode::serialize(&key)?;
        <Self as Store<State>>::del(self, serialized_key, msg)
    }

    pub fn storage(&self) -> Arc<dyn Store<State>> {
        self.data.clone()
    }

    fn new_storage(
        app_name: String,
        storage_url: Url,
        table_name: String,
    ) -> Result<Arc<dyn Store<State>>> {
        match storage_url.scheme().to_lowercase().as_str() {
            "inmemory" => {
                let db = InMemoryStore::new(app_name, storage_url, table_name);
                Ok(Arc::new(db))
            }
            "rocksdb" | "rocks" => {
                #[cfg(feature = "store-rocksdb")]
                {
                    use crate::stores::rocksdb::RocksDbStore;
                    let rdb = RocksDbStore::new(app_name, storage_url, table_name);
                    return Ok(Arc::new(rdb));
                }

                Err(CallystoError::GeneralError(
                    "RocksDB feature is not enabled. `store-rocksdb` is the feature name.".into(),
                ))
            }
            "aerospikedb" | "aerospike" => todo!(),
            storage_backend => Err(CallystoError::GeneralError(format!(
                "Unknown storage backend: {}",
                storage_backend
            ))),
        }
    }

    fn on_changelog_sent(&self) -> Result<()> {
        match self.config.kafka_config.processing_guarantee {
            ProcessingGuarantee::AtLeastOnce => {
                todo!()
            }
            ProcessingGuarantee::ExactlyOnce => {
                todo!()
            }
        }
        todo!()
    }

    fn verify_source_topic_partitions(&self) -> Result<()> {
        let source_topic_ccontext = self
            .source_topic_consumer_context
            .get()
            .as_ref()
            .to_owned()
            .unwrap();
        let source_topic_name = source_topic_ccontext.topic_name.clone();
        let source_topic_stats = source_topic_ccontext.get_stats();
        match &*source_topic_stats {
            Some(stat) => {
                let source_topic_meta =
                    stat.topics.get(source_topic_name.as_str()).ok_or_else(|| {
                        CallystoError::GeneralError("Source topic is not found in metadata.".into())
                    })?;
                // dbg!(&source_topic_meta);
                let changelog_topic_meta = stat
                    .topics
                    .get(self.changelog_topic.topic_name().as_str())
                    .ok_or_else(|| {
                        CallystoError::NoTopic(
                            self.changelog_topic.topic_name(),
                            "Changelog topic is not found in metadata.".into(),
                        )
                    })?;
                let source_n = source_topic_meta.partitions.len() - 1;
                let changelog_n = changelog_topic_meta.partitions.len() - 1;

                if source_n != changelog_n {
                    info!(
                        "Source <> Changelog partitions are not matching. Source: {}, Changelog: {}",
                        source_n, changelog_n
                    );
                    return Err(CallystoError::GeneralError(
                        "Partition mismatch in changelog vs source topics".into(),
                    ));
                }
            }
            _ => {
                return Err(CallystoError::ConsumerNoStat(
                    "No stat has been received".into(),
                ))
            }
        }
        info!("Source <> Changelog partitions are matching");

        Ok(())
    }

    async fn after_start_callback(&self) -> Result<()> {
        loop {
            match self.verify_source_topic_partitions() {
                Ok(_) => {}
                Err(e) => {
                    error!("{:?}", e);
                    info!("Changelog topic hasn't been created. Creating...");
                    let source_topic_ccontext = self
                        .source_topic_consumer_context
                        .get()
                        .as_ref()
                        .to_owned()
                        .unwrap();

                    let source_topic_name = source_topic_ccontext.topic_name.clone();
                    let source_topic_stats = source_topic_ccontext.get_stats();

                    if let Some(s) = &*source_topic_stats {
                        let source_topic_meta = s
                            .topics
                            .get(source_topic_name.as_str())
                            .ok_or_else(|| {
                                CallystoError::NoTopic(
                                    source_topic_name,
                                    "Source topic is not found in metadata.".into(),
                                )
                            })
                            .unwrap();
                        let source_n = source_topic_meta.partitions.len() - 1;

                        self.changelog_topic
                            .topic_declare(false, false, 0_f64, source_n)
                            .await?;

                        break;
                    }
                }
            }
        }

        Ok(())
    }

    fn start_changelog_worker(&self) -> Result<()> {
        info!(
            "`{}` Changelog worker started!",
            self.changelog_topic_name()
        );
        let rx = self.changelog_rx.clone();
        let topic = self.changelog_topic.clone();

        nuclei::spawn(async move {
            let producer = topic.producer();
            loop {
                if let Ok((partition, serialized_key, serialized_value)) = rx.recv() {
                    let topic_name = topic.topic_name();
                    let serialized_key = serialized_key.clone();
                    let serialized_value = serialized_value.clone();

                    producer
                        .send(topic_name, partition, serialized_key, serialized_value)
                        .await;
                }
            }
        });

        Ok(())
    }

    pub fn info(&self) -> HashMap<String, String> {
        todo!()
    }
}

#[async_trait]
impl<State> Collection<State> for CTable<State>
where
    State: Clone + Send + Sync + 'static,
{
    fn changelog_topic(&self) -> CTopic {
        self.changelog_topic.clone()
    }

    fn set_changelog_topic(&self, changelog_topic: CTopic) {
        todo!()
    }

    fn changelog_topic_name(&self) -> String {
        self.changelog_topic.topic_name()
    }

    async fn send_changelog(
        &self,
        partition: usize,
        serialized_key: Vec<u8>,
        serialized_value: Vec<u8>,
    ) -> Result<()> {
        self.changelog_tx
            .send((partition, serialized_key, serialized_value));
        Ok(())
    }

    fn partition_for_key(&self, key: Vec<u8>, msg: OwnedMessage) -> Result<Option<usize>> {
        self.verify_source_topic_partitions();
        Ok(Some(msg.partition() as usize))
    }
}

#[async_trait]
impl<State> Store<State> for CTable<State>
where
    State: Clone + Send + Sync + 'static,
{
    fn get(&self, serialized_key: Vec<u8>, msg: OwnedMessage) -> Result<Option<Vec<u8>>> {
        self.data.get(serialized_key, msg)
    }

    fn set(
        &self,
        serialized_key: Vec<u8>,
        serialized_val: Vec<u8>,
        msg: OwnedMessage,
    ) -> Result<()> {
        self.data
            .set(serialized_key.clone(), serialized_val.clone(), msg.clone())?;
        let partition_for_key = self
            .partition_for_key(serialized_key.clone(), msg.clone())
            .or_else::<CallystoError, _>(|e| {
                // Since it's failed to verify partition, we still queue for a later time when changelog topic is available.
                // Or any other error occurs.
                error!("{:?}", e);
                Ok(Some(msg.partition() as _))
            })
            .unwrap()
            .unwrap();

        let dispatch = self.clone();
        let clo = async move {
            dispatch
                .send_changelog(partition_for_key, serialized_key, serialized_val)
                .await;
        };

        nuclei::spawn(clo);

        Ok(())
    }

    fn del(&self, serialized_key: Vec<u8>, msg: OwnedMessage) -> Result<()> {
        self.data.del(serialized_key.clone(), msg.clone())?;

        let partition_for_key = self
            .partition_for_key(serialized_key.clone(), msg)?
            .unwrap();

        let dispatch = self.clone();
        let clo = async move {
            dispatch
                .send_changelog(partition_for_key, serialized_key, vec![])
                .await;
        };

        nuclei::spawn(clo);

        Ok(())
    }

    fn table(&self) -> CTable<State> {
        self.clone()
    }

    fn persisted_offset(&self, tp: CTP) -> Result<Option<usize>> {
        self.data.persisted_offset(tp)
    }

    fn set_persisted_offset(&self, tp: CTP, offset: usize) -> Result<()> {
        self.data.set_persisted_offset(tp, offset)
    }

    fn apply_changelog_batch(&self, events: Vec<OwnedMessage>) -> Result<()> {
        self.data.apply_changelog_batch(events)
    }

    fn reset_state(&self) -> Result<()> {
        self.data.reset_state()
    }

    /// Call when cluster is rebalancing.
    async fn on_rebalance(
        &self,
        assigned: Vec<CTP>,
        revoked: Vec<CTP>,
        newly_assigned: Vec<CTP>,
        generation_id: usize,
    ) -> Result<()> {
        self.data
            .on_rebalance(assigned, revoked, newly_assigned, generation_id)
            .await
    }

    /// Call when recovery has completed after rebalancing.
    async fn on_recovery_completed(
        &self,
        active_tps: Vec<CTP>,
        standby_tps: Vec<CTP>,
    ) -> Result<()> {
        self.data
            .on_recovery_completed(active_tps, standby_tps)
            .await;
        // self.call_recover_callbacks()
        todo!()
    }

    fn into_service(&self) -> &dyn Service<State> {
        self
    }
}

#[async_trait]
impl<State> Service<State> for CTable<State>
where
    State: Clone + Send + Sync + 'static,
{
    async fn call(&self, st: Context<State>) -> Result<State> {
        Ok(st.state().clone())
    }

    async fn start(&self) -> Result<BoxFuture<'_, ()>> {
        let closure = async move {
            info!(
                "Started - Table `{}` - Changelog Topic `{}`",
                self.table_name,
                self.changelog_topic_name()
            );
            self.data.start().await;
            // Start changelog worker
            self.start_changelog_worker();
        };

        Ok(closure.boxed())
    }

    async fn after_start(&self) -> Result<()> {
        info!(
            "After Start - Table `{}` - Changelog Topic `{}`",
            self.table_name,
            self.changelog_topic_name()
        );
        self.after_start_callback().await;
        Ok(())
    }

    async fn wait_until_stopped(&self) {
        todo!()
    }

    async fn state(&self) -> String {
        todo!()
    }

    async fn label(&self) -> String {
        format!(
            "{}@{}",
            self.shortlabel().await,
            self.data.shortlabel().await
        )
    }

    async fn shortlabel(&self) -> String {
        format!("table:{}", self.table_name)
    }
}
