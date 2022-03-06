use rdkafka::message::OwnedHeaders;
use rdkafka::producer::future_producer::OwnedDeliveryResult;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use std::time::Duration;

pub struct CProducer {
    config: ClientConfig,
    producer: FutureProducer,
}

impl CProducer {
    pub fn new(config: ClientConfig) -> Self {
        let producer: FutureProducer = config.create().expect("Producer creation error");
        Self { config, producer }
    }

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
}
