use crate::config::Config;
use crate::definitions::Context;
use crate::errors::*;
use crate::kafka::ctopic::{CTopic, CTP};
use crate::kafka::enums::ProcessingGuarantee;
use crate::service::{Service, ServiceState};
use crate::stores::rocksdb::RocksDbStore;
use crate::stores::store::Store;
use async_trait::async_trait;
use lightproc::prelude::State;
use rdkafka::message::OwnedMessage;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

#[derive(Clone)]
pub struct CTable<State = ()>
where
    State: Clone + Send + Sync + 'static,
{
    table_name: String,
    storage_url: Url,
    config: Config,
    changelog_topic: CTopic,
    data: Arc<dyn Store<State>>,
}

impl<State> CTable<State>
where
    State: Clone + Send + Sync + 'static,
{
    pub fn new(storage_url: Url, table_name: String, config: Config) -> Self {
        todo!()
    }

    pub fn get<K, V>(&self, key: K) -> Result<V>
    where
        K: Serialize,
    {
        todo!()
    }

    pub fn set<K, V>(&self, key: K, value: V) -> Result<()>
    where
        K: Serialize,
        V: Serialize,
    {
        todo!()
    }

    pub fn del<K>(&self, key: K) -> Result<()>
    where
        K: Serialize,
    {
        todo!()
    }

    fn new_storage(storage_url: Url, table_name: String) -> Result<Arc<dyn Store<State>>> {
        match storage_url.scheme().to_lowercase().as_str() {
            "rocksdb" | "rocks" => {
                let rdb = RocksDbStore::new(storage_url, table_name);
                Ok(Arc::new(rdb))
            }
            "aerospikedb" | "aerospike" => todo!(),
            "inmemory" => todo!(),
            storage_backend => Err(CallystoError::GeneralError(format!(
                "Unknown storage backend: {}",
                storage_backend
            ))),
        }
    }

    fn on_changelog_sent(&self) -> Result<()> {
        match self.config.processing_guarantee {
            ProcessingGuarantee::AtLeastOnce => {
                todo!()
            }
            ProcessingGuarantee::ExactlyOnce => {
                todo!()
            }
        }
        todo!()
    }

    pub fn info(&self) -> HashMap<String, String> {
        todo!()
    }
}

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

    fn send_changelog(&self, partition: usize, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let topic = self.changelog_topic.clone();

        bastion::executor::blocking(async move {
            let producer = topic.producer();
            let topic_name = topic.topic_name();
            let key = key.clone();
            let value = value.clone();

            producer.send(topic_name, partition, key, value).await
        });

        Ok(())
    }

    fn partition_for_key(&self, key: Vec<u8>) -> Result<usize> {
        todo!()
    }
}

#[async_trait]
impl<State> Store<State> for CTable<State>
where
    State: Clone + Send + Sync + 'static,
{
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
}

#[async_trait]
impl<State> Service<State> for CTable<State>
where
    State: Clone + Send + Sync + 'static,
{
    async fn call(&self, st: Context<State>) -> Result<State> {
        todo!()
    }

    async fn start(&self) -> Result<()> {
        todo!()
    }

    async fn restart(&self) -> Result<()> {
        todo!()
    }

    async fn crash(&self) {
        todo!()
    }

    async fn stop(&self) -> Result<()> {
        todo!()
    }

    async fn wait_until_stopped(&self) {
        todo!()
    }

    async fn started(&self) -> bool {
        todo!()
    }

    async fn crashed(&self) -> bool {
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
        format!("Table: {}", self.table_name)
    }

    async fn service_state(&self) -> Arc<ServiceState> {
        todo!()
    }
}

#[async_trait]
pub trait Collection<State>: Store<State>
where
    State: Clone + Send + Sync + 'static,
{
    /// Get changelog topic
    fn changelog_topic(&self) -> CTopic;

    fn set_changelog_topic(&self, changelog_topic: CTopic);

    fn changelog_topic_name(&self) -> String;

    fn send_changelog(&self, partition: usize, key: Vec<u8>, value: Vec<u8>) -> Result<()>;

    fn partition_for_key(&self, key: Vec<u8>) -> Result<usize>;
}
