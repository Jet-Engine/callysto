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
use lever::sync::atomics::AtomicBox;
use rdkafka::message::OwnedMessage;
use std::collections::HashMap;
use std::future::Future;
use std::io::Read;
use std::sync::Arc;
use http_types::{Request, Response};
use tracing::*;

///
/// Possible states that services can be in.
#[derive(Copy, Clone, PartialOrd, PartialEq, Eq)]
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

///
/// Stateful service definition
pub struct CService<State, F, Fut>
where
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(Context<State>) -> Fut,
    Fut: Future<Output = CResult<State>> + Send + 'static,
{
    service_name: String,
    clo: F,
    state: State,
    dependencies: Vec<Arc<dyn Service<State>>>,
}

impl<State, F, Fut> CService<State, F, Fut>
where
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(Context<State>) -> Fut,
    Fut: Future<Output = CResult<State>> + Send + 'static,
{
    ///
    /// Create new stateful service
    pub fn new<T: AsRef<str>>(service_name: T, service_stub: F, state: State, dependencies: Vec<Arc<dyn Service<State>>>) -> Self {
        Self {
            service_name: service_name.as_ref().to_string(),
            clo: service_stub,
            state,
            dependencies
        }
    }
}

#[async_trait]
impl<State, F, Fut> Service<State> for CService<State, F, Fut>
where
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(Context<State>) -> Fut,
    Fut: Future<Output = CResult<State>> + Send + 'static,
{
    async fn call(&self, st: Context<State>) -> Result<State> {
        let fut = (self.clo)(st);
        let res = fut.await?;
        Ok(res)
    }

    async fn start(&self) -> Result<BoxFuture<'_, ()>> {
        let closure = async move {
            let label = self.label().await;
            for x in &self.dependencies {
                info!("CService - {} - Dependencies are starting", self.service_name);
                x.start().await;
            }
            info!("Started CService {}", label);

            'fallback: loop {
                info!("Launched CService {} worker.", label);
                <Self as Service<State>>::service_state(self)
                    .await
                    .replace_with(|_e| ServiceState::Running);
                'main: loop {
                    if <Self as Service<State>>::stopped(self).await {
                        break 'main;
                    }
                    let state = self.state.clone();
                    let mut context = Context::new(state);
                    match CService::<State, F, Fut>::call(self, context).await {
                        Ok(_) => {}
                        Err(e) => {
                            error!("CService {} failed: {:?}", label, e);
                            <Self as Service<State>>::crash(self).await;
                            break 'main;
                        }
                    }
                }

                if <Self as Service<State>>::stopped(self).await {
                    break 'fallback;
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
        format!("{}@{}", self.service_name, self.shortlabel().await)
    }

    async fn shortlabel(&self) -> String {
        self.service_name.clone()
    }
}
