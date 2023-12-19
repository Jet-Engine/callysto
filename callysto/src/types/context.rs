use super::service::{Service, ServiceState};
use crate::errors::Result as CResult;
use crate::errors::*;
use crate::kafka::ctopic::*;
use crate::prelude::CConsumerContext;
use crate::types::table::CTable;
use async_trait::*;
use futures::future::{BoxFuture, TryFutureExt};
use futures::FutureExt;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use rdkafka::Offset::Offset;
use rdkafka::{Message, TopicPartitionList};
use std::collections::HashMap;
use std::future::Future;
use std::io::Read;
use std::sync::Arc;
use tracing::info;

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
    consumer: Option<Arc<BaseConsumer<CConsumerContext>>>,
}

impl<State> Context<State>
where
    State: Clone + Send + Sync + 'static,
{
    ///
    /// Constructor of the [Context].
    pub fn new(state: State) -> Self {
        Self {
            state,
            consumer: None,
        }
    }

    ///
    /// Borrow state immutably
    pub fn state(&self) -> &State {
        &self.state
    }

    pub(crate) fn set_consumer(&mut self, consumer: Arc<BaseConsumer<CConsumerContext>>) {
        self.consumer = Some(consumer);
    }

    pub(crate) fn get_consumer(&self) -> Option<Arc<BaseConsumer<CConsumerContext>>> {
        self.consumer.clone()
    }

    ///
    /// Check context has the consumer instance or not.
    pub fn has_consumer(&self) -> bool {
        self.consumer.is_some()
    }

    ///
    /// Commit specific offset of a given message with given mode.
    fn commit_internal(&self, msg: OwnedMessage, commit_mode: CommitMode) -> Result<()> {
        let consumer = self.get_consumer().ok_or_else(|| {
            CallystoError::GeneralError("No consumer instance set in context.".into())
        })?;

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(msg.topic(), msg.partition(), Offset(msg.offset()))?;
        consumer.commit(&tpl, commit_mode);

        Ok(())
    }

    ///
    /// Commit specific offset of a given message synchronously.
    /// NOTE: This will also automatically commit every message with lower offset within the same partition.
    pub fn commit(&self, msg: OwnedMessage) -> Result<()> {
        self.commit_internal(msg, CommitMode::Sync)
    }

    ///
    /// Commit specific offset of a given message asynchronously.
    /// NOTE: This will also automatically commit every message with lower offset within the same partition.
    pub async fn commit_async(&self, msg: OwnedMessage) -> Result<()> {
        self.commit_internal(msg, CommitMode::Async)
    }

    ///
    /// Borrow the state mutably.
    pub fn state_mut(&mut self) -> &mut State {
        &mut self.state
    }
}
