use crate::errors::*;
use lever::prelude::TTas;
use lever::sync::atomics::AtomicBox;
use rdkafka::consumer::ConsumerContext;
use rdkafka::{ClientContext, Statistics};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::sync::Arc;
use tracing::{debug, trace};

#[derive(Clone, Debug)]
pub struct CConsumerContext {
    pub topic_name: String,
    statistics: Arc<AtomicBox<Option<CStatistics>>>,
}

impl CConsumerContext {
    pub fn new(topic_name: String) -> Self {
        Self {
            topic_name,
            statistics: Arc::new(AtomicBox::new(None)),
        }
    }

    pub fn get_stats(&self) -> Arc<Option<CStatistics>> {
        self.statistics.get()
    }
}

impl ClientContext for CConsumerContext {
    fn stats(&self, statistics: Statistics) {
        trace!("Statistics received: {:?}", statistics);
        let cstat: CStatistics = unsafe { std::mem::transmute(statistics) };
        self.statistics.replace_with(|_| {
            let cstats = cstat.clone();
            Some(cstats)
        });
    }
}

impl ConsumerContext for CConsumerContext {}

#[derive(Serialize, Deserialize, Clone, Debug)]
///
/// Kafka statistics
#[repr(transparent)]
pub struct CStatistics(pub Statistics);

impl CStatistics {
    ///
    /// Extract statistics.
    pub fn stats(&self) -> &Statistics {
        &self.0
    }
}
