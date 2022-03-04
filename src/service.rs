use crate::definitions::Context;
use crate::errors::*;
use crate::errors::*;
use crate::kafka::ctopic::*;
use async_trait::*;
use futures::future::TryFutureExt;
use std::future::Future;
use std::sync::Arc;

///
/// Possible states that services can be in.
#[derive(Copy, Clone)]
pub enum ServiceState {
    PreStart,
    Running,
    Stopped,
    Restarting,
    Crashed,
}

#[async_trait]
pub trait Service<State>: Send + Sync + 'static
where
    State: Clone + Send + Sync + 'static,
{
    /// Execute the given task with state passed in
    async fn call(&self, st: Context<State>) -> Result<State>;

    async fn start(&self) -> Result<()>;

    async fn restart(&self) -> Result<()>;

    async fn crash(&self);

    async fn stop(&self) -> Result<()>;

    async fn wait_until_stopped(&self);

    async fn started(&self) -> bool;

    async fn crashed(&self) -> bool;

    async fn state(&self) -> String;

    async fn label(&self) -> String;

    async fn shortlabel(&self) -> String;

    // Provider methods
    async fn service_state(&self) -> Arc<ServiceState>;
}

#[async_trait]
impl<State, F, Fut> Service<State> for F
where
    State: Clone + Send + Sync + 'static,
    F: Send + Sync + 'static + Fn(Context<State>) -> Fut,
    Fut: Future<Output = Result<State>> + Send + 'static,
{
    async fn call(&self, req: Context<State>) -> Result<State> {
        let fut = (self)(req);
        let res = fut.await?;
        Ok(res.into())
    }

    async fn start(&self) -> Result<()> {
        todo!()
    }

    async fn restart(&self) -> Result<()> {
        self.stop().and_then(|_| self.start()).await
    }

    async fn crash(&self) {
        todo!()
    }

    async fn stop(&self) -> Result<()> {
        todo!()
    }

    async fn wait_until_stopped(&self) {
        let _ = self.stop().await;
    }

    async fn service_state(&self) -> Arc<ServiceState> {
        unimplemented!("Service state needs to be implemented")
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
}
