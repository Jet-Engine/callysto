use crate::errors::Result as CResult;
use crate::errors::*;
use crate::kafka::ctopic::*;
use crate::service::{Service, ServiceState};
use crate::table::CTable;
use async_trait::*;
use futures::future::TryFutureExt;
use rdkafka::message::OwnedMessage;
use std::collections::HashMap;
use std::future::Future;
use std::io::Read;
use std::sync::Arc;
use tracing::info;

pub type Tables<State> = HashMap<String, CTable<State>>;

#[async_trait]
pub trait Task<State>: Send + Sync + 'static
where
    State: Clone + Send + Sync + 'static,
{
    /// Execute the given task with state passed in
    async fn call(&self, st: Context<State>) -> CResult<State>;
}

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
pub trait Agent<State>: Send + Sync + 'static
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

// #[async_trait]
// impl<State, F, Fut> Service<State> for
// where
//     State: Clone + Send + Sync + 'static,
//     F: Send + Sync + 'static + Fn(Context<State>) -> Fut,
//     Fut: Future<Output = Result<State>> + Send + 'static,
// {
// }

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

    async fn start(&'static self) -> Result<()> {
        for x in &self.dependencies {
            x.start().await;
        }

        let handle = bastion::executor::spawn(async move {
            let consumer = self.topic.consumer();
            info!(
                "Started - Consumer Group `{}` - Topic `{}`",
                self.app_name,
                self.topic.topic_name()
            );
            loop {
                let state = self.state.clone();
                let message = consumer.recv().await;
                let context = Context::new(state);
                let _slow_drop = <Self as Agent<State>>::call(self, message, context)
                    .await
                    .unwrap();
            }
        });

        handle.await;

        Ok(())
    }

    async fn restart(&'static self) -> Result<()> {
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

#[async_trait]
pub trait TableAgent<State>: Send + Sync + 'static
where
    State: Clone + Send + Sync + 'static,
{
    /// Do work on given message with state passed in
    async fn call(
        &self,
        msg: Option<OwnedMessage>,
        tables: Tables<State>,
        st: Context<State>,
    ) -> CResult<()>;
    //
    // fn topic(&self) -> CTopic;
}

#[async_trait]
impl<State, F, Fut> TableAgent<State> for F
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
        let fut = (self)(msg, tables, req);
        let res = fut.await?;
        Ok(res.into())
    }

    // fn topic(&self) -> CTopic {
    //     todo!()
    // }
}

#[async_trait]
impl<State, F, Fut> Task<State> for F
where
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(Context<State>) -> Fut,
    Fut: Future<Output = CResult<State>> + Send + 'static,
{
    async fn call(&self, req: Context<State>) -> CResult<State> {
        let fut = (self)(req);
        let res = fut.await?;
        Ok(res.into())
    }
}

pub struct CService<State>
where
    State: Clone + Send + Sync + 'static,
{
    dependencies: Vec<Arc<dyn Service<State>>>,
}

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
