use futures::future::FutureExt;
use futures::stream::StreamExt;
use futures_timer::Delay;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, MessageStream, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use rdkafka::util::AsyncRuntime;
use rdkafka::ClientConfig;
use std::borrow::Borrow;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tracing::error;

pub struct NucleiRuntime;

impl AsyncRuntime for NucleiRuntime {
    type Delay = futures::future::Map<futures_timer::Delay, fn(())>;

    fn spawn<T>(task: T)
    where
        T: Future<Output = ()> + Send + 'static,
    {
        nuclei::spawn(task);
    }

    fn delay_for(duration: Duration) -> Self::Delay {
        FutureExt::map(Delay::new(duration), |_| ())
    }
}
