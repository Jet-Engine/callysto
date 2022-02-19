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