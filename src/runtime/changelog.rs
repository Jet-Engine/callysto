use crate::errors::*;
use crate::kafka::ctopic::CTP;
use crate::prelude::{Collection, Context, Service, ServiceState};
use crate::stores::store::Store;
use crate::types::table::CTable;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::FutureExt;
use lever::prelude::TTas;
use lever::sync::atomics::AtomicBox;
use rdkafka::message::OwnedMessage;
use rdkafka::Message;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use tracing::info;

#[derive(Copy, Clone, PartialOrd, PartialEq, Serialize, Deserialize)]
pub enum ChangelogOperation {
    Add,
    Discard,
    Update,
}

#[derive(Clone, PartialOrd, PartialEq, Serialize, Deserialize)]
pub struct ChangelogKey {
    pub op: ChangelogOperation,
    pub serialized_key: Vec<u8>,
}

impl ChangelogKey {
    pub fn new(op: ChangelogOperation, serialized_key: Vec<u8>) -> Self {
        Self { op, serialized_key }
    }
}

// pub struct ChangeloggedObject<'a> {
//     pub key: &'a str,
// }
//
// impl<'a> ChangeloggedObject<'a> {
//     pub fn apply_changelog_event<V>(&self, operation: ChangelogOperation, value: V) -> Result<()>
//     where
//         V: Serialize,
//     {
//         use ChangelogOperation::*;
//         match operation {
//             Add => {}
//             Discard => {}
//             Update => {}
//             _ => {}
//         }
//         todo!()
//     }
// }

#[derive(Clone)]
pub struct ChangelogManager<State>
where
    State: Clone + Send + Sync + 'static,
{
    table: CTable<State>,
    dirty_markers: Arc<TTas<HashSet<Vec<u8>>>>,
    storage: Arc<dyn Store<State>>,
    dependencies: Vec<Arc<dyn Service<State>>>,
}

impl<State> ChangelogManager<State>
where
    State: Clone + Send + Sync + 'static,
{
    pub fn new(table: CTable<State>) -> Self {
        let storage = table.storage();
        // XXX: Use this table as a service when trait upcasting is going to be available in the stable compiler.
        let table_service: Arc<dyn Service<State>> = Arc::new(table.clone());

        Self {
            table,
            storage,
            dirty_markers: Arc::new(TTas::new(HashSet::<Vec<u8>>::with_capacity(1 << 7))),
            dependencies: vec![table_service],
        }
    }

    pub async fn send_changelog_event<K, V>(
        &self,
        msg: OwnedMessage,
        key: K,
        operation: ChangelogOperation,
        value: V,
    ) -> Result<()>
    where
        K: Serialize,
        V: Serialize,
    {
        let serialized_key = bincode::serialize(&key)?;
        let serialized_value = bincode::serialize(&value)?;
        if let Some(mut dm) = self.dirty_markers.try_lock() {
            dm.insert(serialized_key.clone());
        }

        let key = ChangelogKey::new(operation, serialized_key);
        let clog_key = bincode::serialize(&key)?;

        self.table
            .send_changelog(msg.partition() as _, clog_key, serialized_value)
            .await?;

        Ok(())
    }
}

#[async_trait]
impl<State> Store<State> for ChangelogManager<State>
where
    State: Clone + Send + Sync + 'static,
{
    fn get(&self, serialized_key: Vec<u8>, msg: OwnedMessage) -> Result<Option<Vec<u8>>> {
        self.storage.get(serialized_key, msg)
    }

    fn set(
        &self,
        serialized_key: Vec<u8>,
        serialized_val: Vec<u8>,
        msg: OwnedMessage,
    ) -> Result<()> {
        self.storage.set(serialized_key, serialized_val, msg)
    }

    fn del(&self, serialized_key: Vec<u8>, msg: OwnedMessage) -> Result<()> {
        self.storage.del(serialized_key, msg)
    }

    fn table(&self) -> CTable<State> {
        self.table.clone()
    }

    fn persisted_offset(&self, tp: CTP) -> Result<Option<usize>> {
        self.storage.persisted_offset(tp)
    }

    fn set_persisted_offset(&self, tp: CTP, offset: usize) -> Result<()> {
        self.storage.set_persisted_offset(tp, offset)
    }

    /// Apply batch of changelog events to local state.
    fn apply_changelog_batch(&self, events: Vec<OwnedMessage>) -> Result<()> {
        // TODO: ChangelogOperation will be used for SetTable
        // self.storage.apply_changelog_batch(events);
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

    fn into_service(&self) -> &dyn Service<State> {
        self
    }
}

#[async_trait]
impl<State> Service<State> for ChangelogManager<State>
where
    State: Clone + Send + Sync + 'static,
{
    async fn call(&self, st: Context<State>) -> Result<State> {
        todo!()
    }

    async fn start(&self) -> Result<BoxFuture<'_, ()>> {
        let closure = async move {
            for x in &self.dependencies {
                info!(
                    "ChangelogManager - {} - Dependencies are starting",
                    self.shortlabel().await
                );
                if !x.started().await {
                    x.start().await;
                }
            }
        };

        Ok(closure.boxed())
    }

    async fn wait_until_stopped(&self) {
        todo!()
    }

    async fn state(&self) -> String {
        todo!()
    }

    async fn label(&self) -> String {
        format!("{}@{}", self.table.app_name, self.shortlabel().await)
    }

    async fn shortlabel(&self) -> String {
        format!("changelog-manager:{}", self.table.table_name)
    }
}
