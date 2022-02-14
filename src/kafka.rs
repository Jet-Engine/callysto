use std::borrow::Borrow;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use futures::future::{FutureExt};
use futures_timer::Delay;
use std::time::Duration;
use futures::stream::StreamExt;
use rdkafka::ClientConfig;
use rdkafka::consumer::{DefaultConsumerContext, MessageStream, StreamConsumer, Consumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use rdkafka::util::AsyncRuntime;
use tracing::error;


pub struct BastionRuntime;

impl AsyncRuntime for BastionRuntime {
    type Delay = futures::future::Map<futures_timer::Delay, fn(())>;

    fn spawn<T>(task: T)
        where
            T: Future<Output = ()> + Send + 'static,
    {
        let _ = bastion::io::spawn(task);
    }

    fn delay_for(duration: Duration) -> Self::Delay {
        FutureExt::map(Delay::new(duration), |_| ())
    }
}

pub struct CConsumer
{
    consumer: StreamConsumer<DefaultConsumerContext, BastionRuntime>,
}

impl CConsumer {
    pub fn stream(&self) -> MessageStream<'_, DefaultConsumerContext, BastionRuntime> {
        self.consumer.stream()
    }

    pub async fn recv(&self) -> Option<OwnedMessage> {
        let mut stream = self.stream();
        let m: Option<KafkaResult<BorrowedMessage>> = stream.next().await;
        m.map_or(None, |r| {
            match r {
                Ok(bm) => Some(bm.detach()),
                Err(e) => {
                    error!("{}", e);
                    None
                }
            }
        })
    }
}

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