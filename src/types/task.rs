use super::context::Context;
use super::service::{Service, ServiceState};
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
use tracing_subscriber::filter::FilterExt;

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
