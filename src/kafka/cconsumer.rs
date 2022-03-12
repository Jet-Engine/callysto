use std::borrow::Borrow;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use futures::future::FutureExt;
use futures::stream::StreamExt;
use futures_timer::Delay;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, MessageStream, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use rdkafka::util::AsyncRuntime;
use rdkafka::ClientConfig;
use tracing::error;

use crate::kafka::runtime::BastionRuntime;

pub struct CConsumer {
    pub(super) consumer: StreamConsumer<DefaultConsumerContext, BastionRuntime>,
}

impl CConsumer {
    pub fn stream(&self) -> MessageStream<'_, DefaultConsumerContext, BastionRuntime> {
        self.consumer.stream()
    }

    pub async fn recv(&self) -> Option<OwnedMessage> {
        let mut stream = self.stream();
        let m: Option<KafkaResult<BorrowedMessage>> = stream.next().await;
        m.map_or(None, |r| match r {
            Ok(bm) => Some(bm.detach()),
            Err(e) => {
                error!("{}", e);
                None
            }
        })
    }
}
