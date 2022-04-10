use super::context::Context;
use super::service::{Service, ServiceState};
use crate::errors::Result as CResult;
use crate::errors::*;
use crate::kafka::ctopic::*;
use crate::types::table::CTable;
use async_trait::*;
use futures::future::{BoxFuture, TryFutureExt};
use futures::FutureExt;
use rdkafka::message::OwnedMessage;
use std::collections::HashMap;
use std::future::Future;
use std::io::Read;
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::filter::FilterExt;

///////////////////////////////////////////////////
//////// CTask
///////////////////////////////////////////////////

pub struct CTask<State, F, Fut>
where
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(Context<State>) -> Fut,
    Fut: Future<Output = CResult<()>> + Send + 'static,
{
    clo: F,
    app_name: String,
    state: State,
}

impl<State, F, Fut> CTask<State, F, Fut>
where
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(Context<State>) -> Fut,
    Fut: Future<Output = CResult<()>> + Send + 'static,
{
    pub fn new(clo: F, app_name: String, state: State) -> Self {
        Self {
            clo,
            app_name,
            state,
        }
    }
}

#[async_trait]
pub trait Task<State>: Send + Sync + 'static
where
    State: Clone + Send + Sync + 'static,
{
    /// Execute the given task with state passed in
    async fn call(&self, st: Context<State>) -> CResult<()>;

    async fn start(&self) -> Result<BoxFuture<'_, ()>> {
        unimplemented!()
    }
}

#[async_trait]
impl<State, F, Fut> Task<State> for CTask<State, F, Fut>
where
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(Context<State>) -> Fut,
    Fut: Future<Output = CResult<()>> + Send + 'static,
{
    async fn call(&self, req: Context<State>) -> CResult<()> {
        let fut = (self.clo)(req);
        let res = fut.await?;
        Ok(res)
    }

    async fn start(&self) -> Result<BoxFuture<'_, ()>> {
        let state = self.state.clone();
        let closure = async move {
            info!("Started CTask - App `{}`", self.app_name.clone());

            let context = Context::new(state);
            if let Err(e) = Task::<State>::call(self, context).await {
                error!("CTask failed: {}", e);
            }
        };

        Ok(closure.boxed())
    }
}
