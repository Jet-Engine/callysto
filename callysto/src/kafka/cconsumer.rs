use crossbeam_channel::{Receiver, RecvError, Sender};
use std::borrow::{Borrow, BorrowMut};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::kafka::contexts::CConsumerContext;
use cuneiform_fields::prelude::*;
use futures::future::FutureExt;
use futures::stream::{BoxStream, StreamExt, Unfold};
use futures::{pin_mut, stream, Stream};
use futures_timer::Delay;
use lever::prelude::TTas;
use lever::sync::ifaces::LockIface;
use pin_project_lite::pin_project;
use rdkafka::consumer::{
    BaseConsumer, Consumer, DefaultConsumerContext, MessageStream, StreamConsumer,
};
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use rdkafka::util::AsyncRuntime;
use rdkafka::ClientConfig;
use std::sync::{Arc, Mutex};
use std::thread;
use tracing::error;

use crate::kafka::runtime::NucleiRuntime;
use crate::prelude::CStatistics;

pub struct CConsumer {
    pub(super) consumer: Arc<BaseConsumer<CConsumerContext>>,
    pub consumer_context: CConsumerContext,
    pub tx: ArchPadding<Sender<Option<OwnedMessage>>>,
    pub rx: ArchPadding<Receiver<Option<OwnedMessage>>>,
}

pin_project! {
    #[derive(Clone)]
    #[must_use = "you need to poll streams otherwise it won't work"]
    pub struct CStream {
        pub context: Arc<CConsumerContext>,
        #[pin]
        rx: ArchPadding<Receiver<Option<OwnedMessage>>>
    }
}

impl CStream {
    ///
    /// Get consumer context.
    pub fn context(&self) -> Arc<CConsumerContext> {
        self.context.clone()
    }

    ///
    /// Topic name
    pub fn topic_name(&self) -> String {
        self.context.topic_name.clone()
    }

    ///
    /// Consumer statistics
    pub fn stats(&self) -> Arc<Option<CStatistics>> {
        self.context.get_stats()
    }
}

impl Stream for CStream {
    type Item = Option<OwnedMessage>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if let Ok(mut inner) = this.rx.recv() {
            // XXX: Always return something, inner Kafka message or no message at all.
            // You can unwrap safely on `Next` stream iterator.
            Poll::Ready(Some(inner))
        } else {
            Poll::Pending
        }
    }
}

impl CConsumer {
    pub(crate) fn consumer_instance(&self) -> Arc<BaseConsumer<CConsumerContext>> {
        self.consumer.clone()
    }

    pub fn cstream(&self) -> CStream {
        let (tx, rx) = (self.tx.clone(), self.rx.clone());
        let consumer = self.consumer_instance();
        Self::gen_stream(tx, rx, consumer)
    }

    ///
    /// Generate stream on demand with unbounded queues.
    fn gen_stream(
        tx: ArchPadding<Sender<Option<OwnedMessage>>>,
        rx: ArchPadding<Receiver<Option<OwnedMessage>>>,
        consumer: Arc<BaseConsumer<CConsumerContext>>,
    ) -> CStream {
        let context = consumer.context().clone();

        let handle = thread::Builder::new()
            .name("cstream-gen".into())
            .spawn(move || {
                for m in consumer.iter() {
                    let msg = match m {
                        Ok(bm) => Some(bm.detach()),
                        Err(e) => {
                            error!("{}", e);
                            None
                        }
                    };

                    tx.send(msg);
                }
            });

        CStream { context, rx }
    }
}
