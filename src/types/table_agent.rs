use super::context::Context;
use super::service::{Service, ServiceState};
use crate::errors::Result as CResult;
use crate::errors::*;
use crate::kafka::ctopic::*;
use crate::types::table::CTable;
use async_trait::*;
use futures::future::{BoxFuture, TryFutureExt};
use futures::FutureExt;
use futures_lite::StreamExt;
use lever::prelude::LOTable;
use lever::sync::atomics::AtomicBox;
use rdkafka::message::OwnedMessage;
use std::collections::HashMap;
use std::future::Future;
use std::io::Read;
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::filter::FilterExt;

pub type Tables<State> = HashMap<String, CTable<State>>;

///////////////////////////////////////////////////
//////// CTableAgent
///////////////////////////////////////////////////

pub struct CTableAgent<State, F, Fut>
where
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(Option<OwnedMessage>, Tables<State>, Context<State>) -> Fut,
    Fut: Future<Output = CResult<()>> + Send + 'static,
{
    clo: F,
    app_name: String,
    agent_name: String,
    state: State,
    topic: CTopic,
    tables: Tables<State>,
    dependencies: Vec<Arc<dyn Service<State>>>,
}

impl<State, F, Fut> CTableAgent<State, F, Fut>
where
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(Option<OwnedMessage>, Tables<State>, Context<State>) -> Fut,
    Fut: Future<Output = CResult<()>> + Send + 'static,
{
    pub fn new(
        clo: F,
        app_name: String,
        agent_name: String,
        state: State,
        topic: CTopic,
        tables: Tables<State>,
        dependencies: Vec<Arc<dyn Service<State>>>,
    ) -> Self {
        Self {
            clo,
            app_name,
            agent_name,
            state,
            topic,
            tables,
            dependencies,
        }
    }

    pub fn topic(&self) -> CTopic {
        self.topic.clone()
    }
}

#[async_trait]
pub trait TableAgent<State>: Service<State> + Send + Sync + 'static
where
    State: Clone + Send + Sync + 'static,
{
    /// Do work on given message, tables with state passed in
    async fn call(
        &self,
        msg: Option<OwnedMessage>,
        tables: Tables<State>,
        st: Context<State>,
    ) -> CResult<()>;
}

#[async_trait]
impl<State, F, Fut> TableAgent<State> for CTableAgent<State, F, Fut>
where
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(Option<OwnedMessage>, Tables<State>, Context<State>) -> Fut,
    Fut: Future<Output = CResult<()>> + Send + 'static,
{
    async fn call(
        &self,
        msg: Option<OwnedMessage>,
        tables: Tables<State>,
        req: Context<State>,
    ) -> CResult<()> {
        let fut = (self.clo)(msg, tables, req);
        let res = fut.await?;
        Ok(res)
    }
}

#[async_trait]
impl<State, F, Fut> Service<State> for CTableAgent<State, F, Fut>
where
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(Option<OwnedMessage>, Tables<State>, Context<State>) -> Fut,
    Fut: Future<Output = CResult<()>> + Send + 'static,
{
    async fn call(&self, st: Context<State>) -> Result<State> {
        Ok(self.state.clone())
    }

    async fn start(&self) -> Result<BoxFuture<'_, ()>> {
        let closure = async move {
            let consumer = self.topic.consumer();

            info!(
                "Assigning consumer contexts for source topic `{}` statistics",
                self.topic.topic_name()
            );
            // Assign consumer contexts to get the statistics data
            self.tables.iter().for_each(|(name, table)| {
                table
                    .source_topic_consumer_context
                    .replace_with(|_| Some(consumer.consumer_context.clone()));
            });
            info!(
                "Assigned consumer contexts for source topic `{}` statistics",
                self.topic.topic_name()
            );

            for (table_name, table) in &self.tables {
                table.start().await.unwrap().await;
                info!("CTable: `{}` table launched", table_name);
            }

            info!(
                "Started Table Agent - Consumer Group `{}` - Topic `{}`",
                self.app_name,
                self.topic.topic_name()
            );
            self.service_state()
                .await
                .replace_with(|e| ServiceState::Running);

            'fallback: loop {
                info!("Launched CTableAgent executor.");
                'main: loop {
                    if self.stopped().await {
                        break 'main;
                    }
                    let state = self.state.clone();
                    let tables = self.tables.clone();
                    let message = consumer.cstream().next().await.unwrap();
                    if message.is_none() {
                        // Error while receiving from Kafka.
                        break 'fallback;
                    }
                    let context = Context::new(state);
                    if let Err(e) = TableAgent::<State>::call(self, message, tables, context).await
                    {
                        error!("CTableAgent failed: {}", e);
                        self.crash().await;
                        break 'main;
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
        format!("table-agent:{}", self.agent_name)
    }
}
