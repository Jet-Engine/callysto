use super::service::{Service, ServiceState};
use crate::errors::Result as CResult;
use crate::errors::*;
use crate::kafka::ctopic::*;
use crate::types::table::CTable;
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
//////// Context
///////////////////////////////////////////////////

///
/// Context passed to every closure of every module definition
#[derive(Clone)]
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
