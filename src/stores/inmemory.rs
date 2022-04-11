use crate::errors::*;
use crate::kafka::ctopic::CTP;
use crate::prelude::{CTable, Context, Service, ServiceState};
use crate::stores::store::Store;
use async_trait::*;
use futures::future::{BoxFuture, FutureExt, TryFutureExt};
use lever::prelude::LOTable;
use lever::sync::atomics::AtomicBox;
use rdkafka::message::OwnedMessage;
use rdkafka::Message;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tracing::info;
use url::Url;

const CALLYSTO_OFFSET_KEY: &[u8; 19] = b"__callysto\0offset__";

pub type InMemoryDb = LOTable<Vec<u8>, Vec<u8>>;

#[derive(Clone)]
pub struct InMemoryStore {
    app_name: String,
    storage_url: Url,
    table_name: String,
    service_state: Arc<AtomicBox<ServiceState>>,
    dbs: LOTable<usize, InMemoryDb>,
}

impl InMemoryStore {
    pub fn new(app_name: String, storage_url: Url, table_name: String) -> Self {
        Self {
            app_name,
            storage_url,
            table_name,
            service_state: Arc::new(AtomicBox::new(ServiceState::PreStart)),
            dbs: LOTable::default(),
        }
    }

    fn db_for_partition(&self, partition: usize) -> Result<Arc<InMemoryDb>> {
        match self.dbs.get(&partition) {
            Some(x) => Ok(Arc::from(x)),
            _ => {
                let db = LOTable::default();
                let _ = self.dbs.insert(partition, db.clone());
                Ok(Arc::from(db))
            }
        }
    }
}

#[async_trait]
impl<State> Service<State> for InMemoryStore
where
    State: Clone + Send + Sync + 'static,
{
    async fn call(&self, st: Context<State>) -> Result<State> {
        unimplemented!()
    }

    async fn start(&self) -> Result<BoxFuture<'_, ()>> {
        let closure = async move {
            info!("InMemory backend is started.");
            self.service_state.replace_with(|_| ServiceState::Running);
        };

        Ok(closure.boxed())
    }

    async fn restart(&self) -> Result<()> {
        <Self as Service<State>>::stop(self)
            .and_then(|_| <Self as Service<State>>::start(self))
            .await;

        Ok(())
    }

    async fn crash(&self) {
        <Self as Service<State>>::service_state(self)
            .await
            .replace_with(|e| ServiceState::Crashed);
    }

    async fn wait_until_stopped(&self) {
        todo!()
    }

    async fn started(&self) -> bool {
        *<Self as Service<State>>::service_state(self).await.get() == ServiceState::Running
    }

    async fn stopped(&self) -> bool {
        *<Self as Service<State>>::service_state(self).await.get() == ServiceState::Stopped
    }

    async fn crashed(&self) -> bool {
        *<Self as Service<State>>::service_state(self).await.get() == ServiceState::Crashed
    }

    async fn state(&self) -> String {
        unimplemented!()
    }

    async fn label(&self) -> String {
        format!(
            "{}@{}",
            self.app_name,
            <Self as Service<State>>::shortlabel(self).await
        )
    }

    async fn shortlabel(&self) -> String {
        format!("inmemory:{}", self.table_name)
    }

    async fn service_state(&self) -> Arc<AtomicBox<ServiceState>> {
        self.service_state.clone()
    }
}

#[async_trait]
impl<State> Store<State> for InMemoryStore
where
    State: Clone + Send + Sync + 'static,
{
    fn get(&self, serialized_key: Vec<u8>, msg: OwnedMessage) -> Result<Option<Vec<u8>>> {
        let partition: usize = msg.partition() as _;
        let db = self.db_for_partition(partition)?;
        match db.get(&serialized_key) {
            Some(x) => Ok(Some(bincode::deserialize(x.as_slice())?)),
            _ => Ok(None),
        }
    }

    fn set(
        &self,
        serialized_key: Vec<u8>,
        serialized_val: Vec<u8>,
        msg: OwnedMessage,
    ) -> Result<()> {
        let partition: usize = msg.partition() as _;
        let db = self.db_for_partition(partition)?;
        let _ = db
            .insert(serialized_key, serialized_val)
            .map_err(|e| CallystoError::GeneralError(format!("{}", e)))?;
        Ok(())
    }

    fn del(&self, serialized_key: Vec<u8>, msg: OwnedMessage) -> Result<()> {
        let partition: usize = msg.partition() as _;
        let db = self.db_for_partition(partition)?;
        let _ = db
            .remove(&serialized_key)
            .map_err(|e| CallystoError::GeneralError(format!("{}", e)))?;
        Ok(())
    }

    fn table(&self) -> CTable<State> {
        unimplemented!("Table needs to be implemented on top of Storage.")
    }

    fn persisted_offset(&self, tp: CTP) -> Result<Option<usize>> {
        let offset = self
            .db_for_partition(tp.partition)?
            .get(&CALLYSTO_OFFSET_KEY.to_vec())
            .ok_or_else(|| CallystoError::GeneralError("Offset fetch failed.".into()))
            .map_or(None, |e| {
                Option::from(usize::from_ne_bytes(e.as_slice().try_into().unwrap()))
            });
        Ok(offset)
    }

    fn set_persisted_offset(&self, tp: CTP, offset: usize) -> Result<()> {
        let _ = self
            .db_for_partition(tp.partition)?
            .insert(
                CALLYSTO_OFFSET_KEY.to_vec(),
                Vec::from(offset.to_ne_bytes()),
            )
            .map_err(|e| CallystoError::GeneralError(format!("{}", e)))?;
        Ok(())
    }

    fn apply_changelog_batch(&self, events: Vec<OwnedMessage>) -> Result<()> {
        let mut tp_offsets: HashMap<CTP, usize> = HashMap::with_capacity(events.len());
        events.iter().for_each(|e| {
            let tp = CTP::new(e.topic().into(), e.partition() as _);
            let offset: usize = e.offset() as _;
            tp_offsets
                .entry(tp)
                .and_modify(|o| *o = offset.max(*o))
                .or_insert(offset);

            match e.payload() {
                Some(p) => {
                    let db = self
                        .db_for_partition(e.partition() as _)
                        .map_err(|e| {
                            CallystoError::GeneralError("Partition number fetch error".into())
                        })
                        .unwrap();
                    db.insert(e.key().unwrap().to_vec(), p.to_vec())
                        .map_err(|e| CallystoError::GeneralError(format!("{}", e)))
                        .unwrap();
                }
                _ => {
                    let db = self
                        .db_for_partition(e.partition() as _)
                        .map_err(|e| {
                            CallystoError::GeneralError("Partition number fetch error".into())
                        })
                        .unwrap();
                    db.remove(&e.key().unwrap().to_vec())
                        .map_err(|e| CallystoError::GeneralError(format!("{}", e)))
                        .unwrap();
                }
            }
        });

        tp_offsets.into_iter().try_for_each(|(tp, offset)| {
            <Self as Store<State>>::set_persisted_offset(self, tp, offset)
        })?;

        Ok(())
    }

    fn reset_state(&self) -> Result<()> {
        let _ = self.dbs.clear();
        Ok(())
    }

    async fn on_rebalance(
        &self,
        assigned: Vec<CTP>,
        revoked: Vec<CTP>,
        newly_assigned: Vec<CTP>,
        generation_id: usize,
    ) -> Result<()> {
        // TODO: On Rebalance
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
