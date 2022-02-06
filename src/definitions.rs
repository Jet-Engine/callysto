use async_trait::*;
use crate::errors::{Result as CResult};
use crate::kafka::CTopic;
use std::future::Future;
use std::sync::Arc;

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
}


#[async_trait]
pub trait Agent<State>: Send + Sync + 'static
where
    State: Clone + Send + Sync + 'static
{
    /// Execute the given task with state passed in
    async fn call(&self, st: Context<State>) -> CResult<State>;

    fn topic(&self) -> CTopic;
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
        F: Send + Sync + 'static + Fn(Context<State>) -> Fut,
        Fut: Future<Output = CResult<State>> + Send + 'static,
{
    async fn call(&self, req: Context<State>) -> CResult<State> {
        let fut = (self)(req);
        let res = fut.await?;
        Ok(res.into())
    }

    fn topic(&self) -> CTopic {
        todo!()
    }
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
}