use async_trait::*;
use crate::errors::{Result as CResult};

#[async_trait]
pub trait TaskDef<State>: Send + Sync + 'static
where
    State: Clone + Send + Sync + 'static
{
    /// Execute the given task with state passed in
    async fn call(&self, st: Context<State>) -> CResult<State>;
}

#[async_trait]
pub trait ServiceDef<State>: Send + Sync + 'static
    where
        State: Clone + Send + Sync + 'static
{
    /// Execute the given task with state passed in
    async fn call(&self, st: Context<State>) -> CResult<State>;
}

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