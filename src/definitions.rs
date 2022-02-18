use async_trait::*;
use crate::errors::{Result as CResult};
use crate::kafka::CTopic;
use std::future::Future;
use std::sync::Arc;
use rdkafka::message::OwnedMessage;

#[async_trait]
pub trait Task<State>: Send + Sync + 'static
where
    State: Clone + Send + Sync + 'static
{
    /// Execute the given task with state passed in
    async fn call(&self, st: Context<State>) -> CResult<State>;
}

#[async_trait]
pub trait Service<State>: Send + Sync + 'static
    where
        State: Clone + Send + Sync + 'static
{
    /// Execute the given task with state passed in
    async fn call(&self, st: Context<State>) -> CResult<State>;

    async fn start(&self);

    async fn restart(&self);

    async fn crash(&self);

    async fn stop(&self);

    async fn wait_until_stopped(&self);

    async fn started(&self) -> bool;

    async fn crashed(&self) -> bool;

    async fn state(&self) -> String;

    async fn label(&self) -> String;

    async fn shortlabel(&self) -> String;
}


#[async_trait]
pub trait Agent<State>: Send + Sync + 'static
where
    State: Clone + Send + Sync + 'static
{
    /// Do work on given message with state passed in
    async fn call(&self, msg: Option<OwnedMessage>, st: Context<State>) -> CResult<()>;
    //
    // fn topic(&self) -> CTopic;
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

#[async_trait]
impl<State, F, Fut> Agent<State> for F
    where
        State: Clone + Send + Sync + 'static,
        F: Send + Sync + 'static + Fn(Option<OwnedMessage>, Context<State>) -> Fut,
        Fut: Future<Output = CResult<()>> + Send + 'static,
{
    async fn call(&self, msg: Option<OwnedMessage>, req: Context<State>) -> CResult<()> {
        let fut = (self)(msg, req);
        let res = fut.await?;
        Ok(res.into())
    }

    // fn topic(&self) -> CTopic {
    //     todo!()
    // }
}

#[async_trait]
impl<State, F, Fut> Service<State> for F
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

pub struct CronJob<Store>
{
    cron_expr: String,
    pub job: Box<dyn Task<Store>>
}

impl<Store> CronJob<Store>
where
    Store: Clone + Send + Sync + 'static
{
    pub fn new<T: AsRef<str>>(cron_expr: T, job: impl Task<Store>) -> Self {
        Self { cron_expr: cron_expr.as_ref().to_owned(), job: Box::new(job) }
    }
}

///
/// Context passed to every closure of every module definition
pub struct Context<State>
where
    State: Clone + Send + Sync + 'static
{
    state: State
}

impl<State> Context<State>
where
    State: Clone + Send + Sync + 'static
{
    pub fn new(state: State) -> Self {
        Self {state}
    }

    pub fn state(&self) -> &State {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut State {
        &mut self.state
    }
}