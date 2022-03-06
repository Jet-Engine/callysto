use crate::errors::Result as CResult;
use crate::errors::*;
use crate::kafka::ctopic::*;
use crate::service::{Service, ServiceState};
use crate::table::CTable;
use async_trait::*;
use futures::future::{BoxFuture, TryFutureExt};
use futures::FutureExt;
use rdkafka::message::OwnedMessage;
use std::collections::HashMap;
use std::future::Future;
use std::io::Read;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::filter::FilterExt;

pub type Tables<State> = HashMap<String, CTable<State>>;

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
// pub trait Agent<State>: Service<State> + Send + Sync + 'static
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

            loop {
                let message = consumer.recv().await;
                let state = state.clone();
                let context = Context::new(state);
                let _slow_drop = Agent::<State>::call(self, message, context).await.unwrap();
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

    async fn service_state(&self) -> Arc<ServiceState> {
        todo!()
    }
}

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
                let state = self.state.clone();
                let tables = self.tables.clone();
                let message = consumer.recv().await;
                let context = Context::new(state);
                let _slow_drop = TableAgent::<State>::call(self, message, tables, context)
                    .await
                    .unwrap();
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

    async fn service_state(&self) -> Arc<ServiceState> {
        todo!()
    }
}

///////////////////////////////////////////////////
//////// CTask
///////////////////////////////////////////////////

#[async_trait]
pub trait Task<State>: Send + Sync + 'static
where
    State: Clone + Send + Sync + 'static,
{
    /// Execute the given task with state passed in
    async fn call(&self, st: Context<State>) -> CResult<State>;
}

// #[async_trait]
// impl<State, F, Fut> Task<State> for F
// where
//     State: Clone + Send + Sync + 'static,
//     F: Send + Sync + 'static + Fn(Context<State>) -> Fut,
//     Fut: Future<Output = CResult<State>> + Send + 'static,
// {
//     async fn call(&self, req: Context<State>) -> CResult<State> {
//         let fut = (self)(req);
//         let res = fut.await?;
//         Ok(res.into())
//     }
// }

///////////////////////////////////////////////////
//////// CService (Custom Service)
///////////////////////////////////////////////////

pub struct CService<State>
where
    State: Clone + Send + Sync + 'static,
{
    dependencies: Vec<Arc<dyn Service<State>>>,
}

///////////////////////////////////////////////////
//////// CronJob
///////////////////////////////////////////////////

pub struct CronJob<State> {
    cron_expr: String,
    pub job: Box<dyn Task<State>>,
}

impl<State> CronJob<State>
where
    State: Clone + Send + Sync + 'static,
{
    pub fn new<T: AsRef<str>>(cron_expr: T, job: impl Task<State>) -> Self {
        Self {
            cron_expr: cron_expr.as_ref().to_owned(),
            job: Box::new(job),
        }
    }
}

///////////////////////////////////////////////////
//////// Context
///////////////////////////////////////////////////

///
/// Context passed to every closure of every module definition
pub struct Context<State>
where
    State: Clone + Send + Sync + 'static,
{
    state: State,
}

impl<State> Context<State>
where
    State: Clone + Send + Sync + 'static,
{
    pub fn new(state: State) -> Self {
        Self { state }
    }

    pub fn state(&self) -> &State {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut State {
        &mut self.state
    }
}
