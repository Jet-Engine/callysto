use super::context::Context;
use crate::errors::Result as CResult;
use crate::errors::*;
use crate::kafka::ctopic::*;
use crate::table::CTable;
use crate::types::service::{Service, ServiceState};
use async_trait::*;
use futures::future::{BoxFuture, TryFutureExt};
use futures::FutureExt;
use lever::sync::atomics::AtomicBox;
use rdkafka::message::OwnedMessage;
use std::collections::HashMap;
use std::future::Future;
use std::io::Read;
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::filter::FilterExt;

///////////////////////////////////////////////////
//////// CAgent
///////////////////////////////////////////////////

pub struct CAgent<State, F, Fut>
where
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(Option<OwnedMessage>, Context<State>) -> Fut,
    Fut: Future<Output = CResult<()>> + Send + 'static,
{
    clo: F,
    app_name: String,
    agent_name: String,
    state: State,
    topic: CTopic,
    dependencies: Vec<Arc<dyn Service<State>>>,
}

impl<State, F, Fut> CAgent<State, F, Fut>
where
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(Option<OwnedMessage>, Context<State>) -> Fut,
    Fut: Future<Output = CResult<()>> + Send + 'static,
{
    pub fn new(
        clo: F,
        app_name: String,
        agent_name: String,
        state: State,
        topic: CTopic,
        dependencies: Vec<Arc<dyn Service<State>>>,
    ) -> Self {
        Self {
            clo,
            app_name,
            agent_name,
            state,
            topic,
            dependencies,
        }
    }

    pub fn topic(&self) -> CTopic {
        self.topic.clone()
    }
}

#[async_trait]
pub trait Agent<State>: Service<State> + Send + Sync + 'static
where
    State: Clone + Send + Sync + 'static,
{
    /// Do work on given message with state passed in
    async fn call(&self, msg: Option<OwnedMessage>, st: Context<State>) -> CResult<()>;
}

#[async_trait]
impl<State, F, Fut> Agent<State> for CAgent<State, F, Fut>
where
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(Option<OwnedMessage>, Context<State>) -> Fut,
    Fut: Future<Output = CResult<()>> + Send + 'static,
{
    async fn call(&self, msg: Option<OwnedMessage>, req: Context<State>) -> CResult<()> {
        let fut = (self.clo)(msg, req);
        let res = fut.await?;
        Ok(res.into())
    }
}

#[async_trait]
impl<State, F, Fut> Service<State> for CAgent<State, F, Fut>
where
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(Option<OwnedMessage>, Context<State>) -> Fut,
    Fut: Future<Output = CResult<()>> + Send + 'static,
{
    async fn call(&self, st: Context<State>) -> Result<State> {
        Ok(self.state.clone())
    }

    async fn start(&self) -> Result<BoxFuture<'_, ()>> {
        let consumer = self.topic.consumer();
        let state = self.state.clone();
        let closure = async move {
            for x in &self.dependencies {
                info!("CAgent - {} - Dependencies are starting", self.agent_name);
                x.start().await;
            }

            info!(
                "Started CAgent {} - Consumer Group `{}` - Topic `{}`",
                self.agent_name,
                self.app_name.clone(),
                self.topic.topic_name()
            );

            'fallback: loop {
                info!("Launched CAgent executor.");
                self.service_state()
                    .await
                    .replace_with(|e| ServiceState::Running);
                'main: loop {
                    if self.stopped().await {
                        break 'main;
                    }
                    let message = consumer.recv().await;
                    if message.is_none() {
                        // Error while receiving from Kafka.
                        break 'fallback;
                    }
                    let state = state.clone();
                    let context = Context::new(state);
                    match Agent::<State>::call(self, message, context).await {
                        Err(e) => {
                            error!("CAgent failed: {}", e);
                            self.crash().await;
                            break 'main;
                        }
                        _ => {}
                    }
                }

                if self.stopped().await {
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
        format!("{}@{}", self.app_name, self.shortlabel().await)
    }

    async fn shortlabel(&self) -> String {
        format!("agent:{}", self.agent_name)
    }
}
