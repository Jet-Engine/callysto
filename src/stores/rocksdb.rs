use std::sync::Arc;
use async_trait::*;
use lever::sync::atomics::AtomicBox;
use rdkafka::message::OwnedMessage;
use crate::definitions::Context;
use crate::errors::Result;
use crate::kafka::ctopic::CTP;
use crate::service::{Service, ServiceState};
use crate::stores::stores::Store;
use futures::future::TryFutureExt;

#[derive(Clone)]
pub struct RocksDbStore {
    service_state: Arc<AtomicBox<ServiceState>>
}

impl RocksDbStore {}

#[async_trait]
impl<State> Service<State> for RocksDbStore
where
    State: Clone + Send + Sync + 'static
{
    async fn call(&self, st: Context<State>) -> Result<State> {
        todo!()
    }

    async fn start(&self) -> Result<()> {
        todo!()
    }

    async fn restart(&self) -> Result<()> {
        <Self as Service<State>>::stop(self)
            .and_then(|_| <Self as Service<State>>::start(self)).await
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
        self.service_state.get()
    }
}

#[async_trait]
impl<State> Store<State> for RocksDbStore
where
    State: Clone + Send + Sync + 'static
{
    fn persisted_offset(&self, tp: CTP) -> Option<usize> {
        todo!()
    }

    fn set_persisted_offset(&mut self, tp: CTP, offset: usize) -> crate::errors::Result<()> {
        todo!()
    }

    fn apply_changelog_batch(&self, events: Vec<OwnedMessage>) -> crate::errors::Result<()> {
        todo!()
    }

    fn reset_state(&self) -> crate::errors::Result<()> {
        todo!()
    }

    async fn on_rebalance(&self, assigned: Vec<CTP>, revoked: Vec<CTP>, newly_assigned: Vec<CTP>, generation_id: usize) -> crate::errors::Result<()> {
        todo!()
    }

    async fn on_recovery_completed(&self, active_tps: Vec<CTP>, standby_tps: Vec<CTP>) -> crate::errors::Result<()> {
        todo!()
    }
}