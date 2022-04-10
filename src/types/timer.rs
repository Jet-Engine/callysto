use super::context::Context;
use super::service::{Service, ServiceState};
use crate::errors::Result as CResult;
use crate::errors::*;
use crate::kafka::ctopic::*;
use crate::prelude::Task;
use crate::types::table::CTable;
use async_trait::*;
use futures::future::{BoxFuture, TryFutureExt};
use futures::FutureExt;
use futures_timer::Delay;
use rdkafka::message::OwnedMessage;
use std::collections::HashMap;
use std::future::Future;
use std::io::Read;
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::filter::FilterExt;

///////////////////////////////////////////////////
//////// CTimer
///////////////////////////////////////////////////

pub struct CTimer<State, F, Fut>
where
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(Context<State>) -> Fut,
    Fut: Future<Output = CResult<()>> + Send + 'static,
{
    clo: F,
    app_name: String,
    interval: f64,
    state: State,
}

impl<State, F, Fut> CTimer<State, F, Fut>
where
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(Context<State>) -> Fut,
    Fut: Future<Output = CResult<()>> + Send + 'static,
{
    pub fn new(clo: F, app_name: String, interval: f64, state: State) -> Self {
        Self {
            clo,
            app_name,
            interval,
            state,
        }
    }
}

#[async_trait]
impl<State, F, Fut> Task<State> for CTimer<State, F, Fut>
where
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(Context<State>) -> Fut,
    Fut: Future<Output = CResult<()>> + Send + 'static,
{
    async fn call(&self, req: Context<State>) -> CResult<()> {
        let fut = (self.clo)(req);
        let res = fut.await?;
        let _ = Delay::new(std::time::Duration::from_secs_f64(self.interval)).await;
        Ok(res)
    }

    async fn start(&self) -> Result<BoxFuture<'_, ()>> {
        let closure = async move {
            info!("Started CTimer - App `{}`", self.app_name.clone());

            loop {
                let state = self.state.clone();
                let context = Context::new(state);
                if let Err(e) = Task::<State>::call(self, context).await {
                    error!("CTimer failed: {}", e);
                }
            }
        };

        Ok(closure.boxed())
    }
}
