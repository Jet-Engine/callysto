use super::context::Context;
use super::service::{Service, ServiceState};
use crate::errors::Result as CResult;
use crate::errors::*;
use crate::kafka::ctopic::*;
use crate::table::CTable;
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
        Ok(res.into())
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
        info!("CTableAgent");
        let closure = async move {
            for (table_name, table) in &self.tables {
                table.start().await.unwrap().await;
                info!("CTable: `{}` table launched", table_name);
            }

            let consumer = self.topic.consumer();
            info!(
                "Started Table Agent - Consumer Group `{}` - Topic `{}`",
                self.app_name,
                self.topic.topic_name()
            );
            loop {
                'main: loop {
                    info!("Launched CTableAgent executor.");
                    let state = self.state.clone();
                    let tables = self.tables.clone();
                    let message = consumer.recv().await;
                    let context = Context::new(state);
                    match TableAgent::<State>::call(self, message, tables, context).await {
                        Err(e) => {
                            error!("CTableAgent failed: {}", e);
                            break 'main;
                        }
                        Ok(_) => {}
                    }
                }
            }
        };

        Ok(closure.boxed())
    }

    async fn restart(&self) -> Result<()> {
        todo!()
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

    async fn service_state(&self) -> Arc<AtomicBox<ServiceState>> {
        Arc::new(AtomicBox::new(ServiceState::PreStart))
    }
}
