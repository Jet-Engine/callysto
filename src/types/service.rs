use crate::errors::Result as CResult;
use crate::errors::*;
use crate::kafka::ctopic::*;
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
use super::context::Context;
use tracing_subscriber::filter::FilterExt;
use crate::errors::*;
use crate::errors::*;
use crate::kafka::ctopic::*;
use async_trait::*;

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

    async fn start(&self) -> Result<BoxFuture<'_, ()>>;

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

///////////////////////////////////////////////////
//////// CService (Custom Service)
///////////////////////////////////////////////////

pub struct CService<State>
    where
        State: Clone + Send + Sync + 'static,
{
    dependencies: Vec<Arc<dyn Service<State>>>,
}
