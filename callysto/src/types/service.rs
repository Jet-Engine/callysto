use super::context::Context;
use crate::errors::Result as CResult;
use crate::errors::*;
use crate::errors::*;
use crate::errors::*;
use crate::kafka::ctopic::*;
use crate::kafka::ctopic::*;
use crate::types::table::CTable;
use async_trait::*;
use async_trait::*;
use futures::future::{BoxFuture, TryFutureExt};
use futures::FutureExt;
use futures_timer::Delay;
use lever::sync::atomics::AtomicBox;
use rdkafka::message::OwnedMessage;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

///
/// Possible states that services can be in.
#[derive(Copy, Debug, Clone, PartialOrd, PartialEq, Eq)]
pub enum ServiceState {
    PreStart,
    Running,
    Halting,
    Stopped,
    Restarting,
    Crashed,
}

impl fmt::Display for ServiceState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ServiceState::PreStart => write!(f, "PreStart"),
            ServiceState::Running => write!(f, "Running"),
            ServiceState::Halting => write!(f, "Halting"),
            ServiceState::Stopped => write!(f, "Stopped"),
            ServiceState::Restarting => write!(f, "Restarting"),
            ServiceState::Crashed => write!(f, "Crashed"),
        }
    }
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

    async fn halt(&self) -> Result<()> {
        self.service_state()
            .await
            .replace_with(|e| ServiceState::Halting);

        Ok(())
    }

    async fn stopped(&self) -> Result<()> {
        self.service_state()
            .await
            .replace_with(|e| ServiceState::Stopped);

        Ok(())
    }

    async fn wait_until_stopped(&self) -> Result<()> {
        while self.is_halting().await {
            Delay::new(Duration::from_millis(200)).await
        }

        Ok(())
    }

    async fn is_started(&self) -> bool {
        *self.service_state().await.get() == ServiceState::Running
    }

    async fn is_halting(&self) -> bool {
        *self.service_state().await.get() == ServiceState::Halting
    }

    async fn is_stopped(&self) -> bool {
        *self.service_state().await.get() == ServiceState::Stopped
    }

    async fn is_crashed(&self) -> bool {
        *self.service_state().await.get() == ServiceState::Crashed
    }

    async fn state(&self) -> String {
        (*self.service_state().await.get()).to_string()
    }

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
