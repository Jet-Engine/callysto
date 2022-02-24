use std::borrow::Borrow;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use futures::future::FutureExt;
use futures::stream::StreamExt;
use futures_timer::Delay;
use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, MessageStream, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use rdkafka::util::AsyncRuntime;
use tracing::error;

use crate::kafka::cconsumer::CConsumer;
use crate::kafka::runtime::BastionRuntime;

#[derive(Clone)]
pub struct CTopic
{
    topic: String,
    client_config: ClientConfig
}

impl CTopic {
    pub fn new<T>(topic: T, client_config: ClientConfig) -> Self
        where
            T: AsRef<str>
    {
        Self {
            topic: topic.as_ref().to_owned(),
            client_config
        }
    }

    pub fn topic_name(&self) -> String {
        self.topic.clone()
    }

    pub fn consumer(&self) -> CConsumer {
        let consumer: StreamConsumer<_, BastionRuntime> = self.client_config
            .create()
            .expect("Consumer creation failed");
        consumer.subscribe(&[&self.topic]).unwrap();

        CConsumer {
            consumer
        }
    }
}

///
/// Topic - Partition tuple
pub struct CTP {
    pub topic: String,
    pub partition: usize
}