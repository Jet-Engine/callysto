use crate::kafka::runtime::*;
use crate::prelude::CConsumerContext;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::future_producer::OwnedDeliveryResult;
use rdkafka::producer::{FutureProducer, FutureRecord, NoCustomPartitioner};
use std::time::Duration;

pub struct CProducer {
    config: ClientConfig,
    producer: FutureProducer<DefaultClientContext, NucleiRuntime, NoCustomPartitioner>,
}

impl CProducer {
    pub fn new(config: ClientConfig) -> Self {
        let producer: FutureProducer<DefaultClientContext, NucleiRuntime, _> =
            config.create().expect("Producer creation error");
        Self { config, producer }
    }

    ///
    /// Customizable send for producer.
    pub async fn send(
        &self,
        topic_name: String,
        partition: usize,
        key: Vec<u8>,
        payload: Vec<u8>,
    ) -> OwnedDeliveryResult {
        self.producer
            .send(
                FutureRecord::to(topic_name.as_str())
                    .partition(partition as _)
                    .payload(payload.as_slice())
                    .key(key.as_slice())
                    .headers(OwnedHeaders::new()),
                Duration::from_secs(0),
            )
            .await
    }

    ///
    /// Send value only to a topic without key being defined.
    pub async fn send_value<T: AsRef<str>>(
        &self,
        topic_name: T,
        payload: Vec<u8>,
    ) -> OwnedDeliveryResult {
        self.producer
            .send::<Vec<u8>, _, _>(
                FutureRecord::to(topic_name.as_ref())
                    .payload(payload.as_slice())
                    .headers(OwnedHeaders::new()),
                Duration::from_secs(0),
            )
            .await
    }
}
