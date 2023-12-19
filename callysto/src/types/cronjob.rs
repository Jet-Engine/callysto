use super::context::Context;
use super::service::{Service, ServiceState};
use crate::errors::Result as CResult;
use crate::errors::*;
use crate::kafka::ctopic::*;
use crate::types::table::CTable;
use crate::types::task::Task;
use async_trait::*;
use futures::future::{BoxFuture, TryFutureExt};
use futures::FutureExt;
use rdkafka::message::OwnedMessage;
use std::collections::HashMap;
use std::future::Future;
use std::io::Read;
use std::sync::Arc;
use tracing::info;

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
