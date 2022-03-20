use super::context::Context;
use crate::errors::Result as CResult;
use crate::errors::*;
use crate::errors::*;
use crate::errors::*;
use crate::kafka::ctopic::*;
use crate::kafka::ctopic::*;
use crate::table::CTable;
use async_trait::*;
use async_trait::*;
use futures::future::{BoxFuture, TryFutureExt};
use futures::FutureExt;
use lever::sync::atomics::AtomicBox;
use rdkafka::message::OwnedMessage;
use std::collections::HashMap;
use std::future::Future;
use std::io::Read;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::filter::FilterExt;

///
/// Possible states that services can be in.
#[derive(Copy, Clone, PartialOrd, PartialEq)]
pub enum ServiceState {
    PreStart,
    Running,
    Stopped,
    Restarting,
    Crashed,
}

#[async_trait]
pub trait Service<State>: Send + Sync + 'static
where
    State: Clone + Send + Sync + 'static,
{
    /// Execute the given task with state passed in
    async fn call(&self, st: Context<State>) -> Result<State>;

    async fn start(&self) -> Result<BoxFuture<'_, ()>>;

    async fn after_start(&self) -> Result<()> {
        Ok(())
    }

    async fn restart(&self) -> Result<()> {
        self.service_state()
            .await
            .replace_with(|e| ServiceState::Restarting);

        Ok(())
    }

    async fn crash(&self) {
        self.service_state()
            .await
            .replace_with(|e| ServiceState::Crashed);
    }

    async fn stop(&self) -> Result<()> {
        self.service_state()
            .await
            .replace_with(|e| ServiceState::Stopped);

        Ok(())
    }

    async fn wait_until_stopped(&self);

    async fn started(&self) -> bool {
        *self.service_state().await.get() == ServiceState::Running
    }

    async fn stopped(&self) -> bool {
        *self.service_state().await.get() == ServiceState::Stopped
    }

    async fn crashed(&self) -> bool {
        *self.service_state().await.get() == ServiceState::Crashed
    }

    async fn state(&self) -> String;

    async fn label(&self) -> String;

    async fn shortlabel(&self) -> String;

    // Provider methods
    async fn service_state(&self) -> Arc<AtomicBox<ServiceState>> {
        Arc::new(AtomicBox::new(ServiceState::PreStart))
    }
}

///////////////////////////////////////////////////
//////// CService (Custom Service)
///////////////////////////////////////////////////

pub struct CService<State>
where
    State: Clone + Send + Sync + 'static,
{
    dependencies: Vec<Arc<dyn Service<State>>>,
}
