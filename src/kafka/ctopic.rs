use std::borrow::Borrow;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use crate::errors::*;
use crate::kafka::cadmin::CAdminClient;
use futures::future::FutureExt;
use futures::stream::StreamExt;
use futures_timer::Delay;
use lever::sync::atomics::AtomicBox;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication, TopicResult};
use rdkafka::consumer::{Consumer, DefaultConsumerContext, MessageStream, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use rdkafka::util::AsyncRuntime;
use rdkafka::ClientConfig;
use tracing::error;

use crate::kafka::cconsumer::CConsumer;
use crate::kafka::contexts::CConsumerContext;
use crate::kafka::cproducer::CProducer;
use crate::kafka::runtime::BastionRuntime;

#[derive(Clone)]
pub struct CTopic {
    topic: String,
    client_config: ClientConfig,
}

impl CTopic {
    pub fn new<T>(topic: T, client_config: ClientConfig) -> Self
    where
        T: AsRef<str>,
    {
        Self {
            topic: topic.as_ref().to_owned(),
            client_config,
        }
    }

    pub fn topic_name(&self) -> String {
        self.topic.clone()
    }

    pub fn consumer(&self) -> CConsumer {
        let consumer_context = CConsumerContext::new(self.topic.clone());
        let consumer: StreamConsumer<_, BastionRuntime> = self
            .client_config
            .create_with_context(consumer_context.clone())
            .expect("Consumer creation failed");
        consumer.subscribe(&[&self.topic]).unwrap();

        CConsumer {
            consumer,
            consumer_context,
        }
    }

    pub fn admin_client(&self) -> CAdminClient {
        CAdminClient::new(
            self.client_config.clone(),
            CConsumerContext::new(self.topic.clone()),
        )
    }

    pub fn producer(&self) -> CProducer {
        CProducer::new(self.client_config.clone())
    }

    pub fn client_config(&self) -> ClientConfig {
        self.client_config.clone()
    }

    pub async fn topic_declare(
        &self,
        compacting: bool,
        deleting: bool,
        retention: f64,
        partitions: usize,
    ) -> Result<Vec<TopicResult>> {
        let manager = self.admin_client().manager();
        let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(1)));
        let topic_name = self.topic.clone();

        let mut topic = NewTopic::new(
            topic_name.as_str(),
            partitions as _,
            TopicReplication::Fixed(1),
        );

        let mut topic = match (compacting, deleting) {
            (true, true) => topic.set("cleanup.policy", "compact,delete"),
            (false, true) => topic.set("cleanup.policy", "delete"),
            (true, false) => topic.set("cleanup.policy", "compact"),
            _ => topic,
        };

        let retention = format!("{}", retention as usize);
        topic = topic.set("retention.ms", retention.as_str());

        Ok(manager.create_topics(&[topic], &opts).await?)
    }
}

///
/// Topic - Partition tuple
#[derive(Eq, PartialEq, Hash)]
pub struct CTP {
    pub topic: String,
    pub partition: usize,
}

impl CTP {
    pub fn new(topic: String, partition: usize) -> Self {
        Self { topic, partition }
    }
}
