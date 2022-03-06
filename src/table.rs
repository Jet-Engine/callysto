use crate::definitions::Context;
use crate::errors::*;
use crate::kafka::ctopic::{CTopic, CTP};
use crate::service::{Service, ServiceState};
use crate::stores::rocksdb::RocksDbStore;
use crate::stores::store::Store;
use async_trait::async_trait;
use lightproc::prelude::State;
use rdkafka::message::OwnedMessage;
use std::sync::Arc;
use url::Url;

#[derive(Clone)]
pub struct CTable<State>
where
    State: Clone + Send + Sync + 'static,
{
    storage_url: Option<Url>,
    changelog_topic: CTopic,
    data: Arc<dyn Store<State>>,
}

impl<State> CTable<State>
where
    State: Clone + Send + Sync + 'static,
{
    pub fn new(storage_url: Url, table_name: String) -> Self {
        todo!()
    }

    fn new_storage(storage_url: Url, table_name: String) -> Result<Arc<dyn Store<State>>> {
        match storage_url.scheme().to_lowercase().as_str() {
            "rocksdb" | "rocks" => {
                let rdb = RocksDbStore::new(storage_url, table_name);
                Ok(Arc::new(rdb))
            }
            "aerospikedb" | "aerospike" => todo!(),
            storage_backend => Err(CallystoError::GeneralError(format!(
                "Unknown storage backend: {}",
                storage_backend
            ))),
        }
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
        todo!()
    }

    fn send_changelog(&self, partition: usize, key: &[u8], value: &[u8]) -> Result<()> {
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
        todo!()
    }

    fn set_persisted_offset(&self, tp: CTP, offset: usize) -> Result<()> {
        todo!()
    }

    fn apply_changelog_batch(&self, events: Vec<OwnedMessage>) -> Result<()> {
        todo!()
    }

    fn reset_state(&self) -> Result<()> {
        todo!()
    }

    async fn on_rebalance(
        &self,
        assigned: Vec<CTP>,
        revoked: Vec<CTP>,
        newly_assigned: Vec<CTP>,
        generation_id: usize,
    ) -> Result<()> {
        todo!()
    }

    async fn on_recovery_completed(
        &self,
        active_tps: Vec<CTP>,
        standby_tps: Vec<CTP>,
    ) -> Result<()> {
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
        todo!()
    }

    async fn shortlabel(&self) -> String {
        todo!()
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

    fn send_changelog(&self, partition: usize, key: &[u8], value: &[u8]) -> Result<()>;
}
