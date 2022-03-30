use crossbeam_channel::{Receiver, RecvError, Sender};
use std::borrow::{Borrow, BorrowMut};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::kafka::contexts::CConsumerContext;
use futures::future::FutureExt;
use futures::stream::{BoxStream, StreamExt, Unfold};
use futures::{pin_mut, stream, Stream};
use futures_timer::Delay;
use lever::prelude::TTas;
use lever::sync::ifaces::LockIface;
use pin_project_lite::pin_project;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, MessageStream, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use rdkafka::util::AsyncRuntime;
use rdkafka::ClientConfig;
use std::sync::{Arc, Mutex};
use tracing::error;

use crate::kafka::runtime::NucleiRuntime;

// pub type CStream = CallystoStream;
// pub type CStream<'a> = impl Stream<Item = Option<OwnedMessage>> + 'a;

type DriveStream<'a> = MessageStream<'a, CConsumerContext, NucleiRuntime>;
//
// pub struct CallystoStream {
//     drive_stream: dyn Stream<Item = Option<OwnedMessage>>
// }
//
// // pub struct CallystoStream {
// //     main_stream: Receiver<Option<OwnedMessage>>
// // }
//
// impl Stream for CallystoStream {
//     type Item = Option<OwnedMessage>;
//
//     fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//         // match self.main_stream.recv() {
//         //     Ok(None) => Poll::Pending,
//         //     Ok(message) => Poll::Ready(Some(message)),
//         //     _ => Poll::Pending
//         // }
//         self.drive_stream.poll_next(cx)
//     }
// }

pub struct CStreamState<S: Stream> {
    main_stream: S,
}

impl<S: Stream> CStreamState<S> {
    pub fn new(main_stream: S) -> Self {
        Self { main_stream }
    }
}

pub struct CConsumer {
    pub(super) consumer: StreamConsumer<CConsumerContext, NucleiRuntime>,
    pub consumer_context: CConsumerContext,
}

// pin_project! {
//     #[derive(Clone)]
//     pub struct CStream {
//         #[pin]
//         drive_stream: BoxStream<'_, Option<OwnedMessage>>
//     }
// }

// #[derive(Clone)]
// pub struct CStream {
//     drive_stream: Arc<dyn Stream<Item=Option<OwnedMessage>>>
// }

pin_project! {
    #[derive(Clone)]
    pub struct CStream {
        #[pin]
        drive_stream: Arc<Mutex<Pin<Box<dyn Stream<Item=Option<OwnedMessage>>>>>>
    }
}

unsafe impl Send for CStream {}

// #[derive(Clone)]
// pub struct CStream {
//     drive_stream: Arc<dyn Stream<Item=Option<OwnedMessage>>>
// }

impl Stream for CStream {
    type Item = Option<OwnedMessage>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // match self.main_stream.recv() {
        //     Ok(None) => Poll::Pending,
        //     Ok(message) => Poll::Ready(Some(message)),
        //     _ => Poll::Pending
        // }

        let mut this = self.project();
        let mut inner = this.drive_stream.lock().unwrap();
        Pin::new(&mut *inner).poll_next(cx)
        // let mut ds = **this.drive_stream;
        // let mut dsg = ds.lock();
        // let mut dsgm = Pin::new(&mut *dsg);
        // dsgm.poll_next(cx)
        // (&mut &*ds).poll_next(cx)
    }
}

impl CConsumer {
    pub fn stream(&self) -> MessageStream<'_, CConsumerContext, NucleiRuntime> {
        self.consumer.stream()
    }

    pub fn cstream(&self) -> CStream {
        let stream: MessageStream<CConsumerContext, NucleiRuntime> = self.consumer.stream();
        let stream: MessageStream<CConsumerContext, NucleiRuntime> =
            unsafe { std::mem::transmute(stream) };
        let state = CStreamState::new(stream);

        // let main_stream: dyn Stream<Item = Option<OwnedMessage>>
        let main_stream = stream::unfold(state, move |mut state| async move {
            'main: loop {
                if let Some(r) = state.main_stream.next().await {
                    let res = match r {
                        Ok(bm) => Some(bm.detach()),
                        Err(e) => {
                            error!("{}", e);
                            None
                        }
                    };
                    break Some((res, state));
                } else {
                    // ERROR
                    break None;
                }
            }
        });

        // let main_stream: Pin<Box<dyn Stream<Item=Option<OwnedMessage>>>> = Box::pin(main_stream);
        // let boxed = main_stream.boxed();
        let boxed: Pin<Box<dyn Stream<Item = Option<OwnedMessage>>>> = Box::pin(main_stream);
        let ark: Arc<Mutex<Pin<Box<dyn Stream<Item = Option<OwnedMessage>>>>>> =
            Arc::new(Mutex::new(boxed));

        CStream { drive_stream: ark }
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
