use crate::errors::*;
use crate::prelude::{Context, ServiceState};
use crate::types::service::Service;
use async_trait::async_trait;
use futures::future::BoxFuture;
use lever::sync::atomics::AtomicBox;
use std::sync::Arc;

pub struct RecoveryService<State>
where
    State: Clone + Send + Sync + 'static,
{
    dependencies: Vec<Arc<dyn Service<State>>>,
}

#[async_trait]
impl<State> Service<State> for RecoveryService<State>
where
    State: Clone + Send + Sync + 'static,
{
    async fn call(&self, st: Context<State>) -> Result<State> {
        todo!()
    }

    async fn start(&self) -> Result<BoxFuture<'_, ()>> {
        todo!()
    }

    async fn wait_until_stopped(&self) {
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

    async fn service_state(&self) -> Arc<AtomicBox<ServiceState>> {
        todo!()
    }
}
