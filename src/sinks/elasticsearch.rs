use crate::errors::*;
use crate::runtime::async_con::Arc;
use crossbeam_channel::{Receiver, Sender};
use cuneiform_fields::arch::ArchPadding;
use elasticsearch::http::transport::Transport;
use elasticsearch::{Elasticsearch, IndexParts};
use futures::sink::{drain, With};
use futures::Future;
use futures::{Sink, SinkExt};
use futures_lite::FutureExt;
use nuclei::Task;
use pin_project_lite::pin_project;
use serde::Serialize;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::vec::Drain;
use tracing::{debug, trace, warn};
use url::Url;

#[derive(Debug)]
pub struct CElasticSearchDocument<T> {
    pub index_id: String,
    pub doc: T,
}

impl<T> CElasticSearchDocument<T>
where
    T: Serialize + Send,
{
    pub fn new(index_id: String, doc: T) -> Self {
        Self { index_id, doc }
    }
}

pin_project! {
    pub struct CElasticSearchSink<T>
    where
        T: Serialize
    {
        index_name: String,
        client: Arc<Elasticsearch>,
        tx: ArchPadding<Sender<CElasticSearchDocument<T>>>,
        buffer_size: usize,
        #[pin]
        data_sink: Task<()>
    }
}

impl<T> CElasticSearchSink<T>
where
    T: Serialize + Send + 'static,
{
    pub fn new(index_name: String, elastic_url: String, buffer_size: usize) -> Result<Self> {
        let transport = Transport::single_node(elastic_url.as_ref())?;
        let client = Elasticsearch::new(transport);

        let (tx, rx) = crossbeam_channel::unbounded::<CElasticSearchDocument<T>>();

        let (tx, rx) = (ArchPadding::new(tx), ArchPadding::new(rx));
        // tx: ArchPadding<Sender<CElasticSearchDocument<T>>>;
        // rx: ArchPadding<Receiver<CElasticSearchDocument<T>>>;

        let idx_name = index_name.clone();
        let client = Arc::new(client);
        let client_copy = client.clone();
        let data_sink = nuclei::spawn(async move {
            while let Ok(item) = rx.recv() {
                let edoc = serde_json::to_string(&item.doc).unwrap();
                let res = client_copy
                    .index(IndexParts::IndexId(
                        idx_name.as_str(),
                        item.index_id.as_str(),
                    ))
                    .body(edoc)
                    .send()
                    .await;
                trace!("CElasticSearchSink - Ingestion status: {:#?}", res);
            }
        });

        Ok(Self {
            index_name,
            client,
            tx,
            buffer_size,
            data_sink,
        })
    }
}

impl<T> Sink<CElasticSearchDocument<T>> for CElasticSearchSink<T>
where
    T: Serialize + Send,
{
    type Error = CallystoError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if self.buffer_size == 0 {
            // Bypass buffering
            return Poll::Ready(Ok(()));
        }

        if self.tx.len() >= self.buffer_size {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn start_send(mut self: Pin<&mut Self>, item: CElasticSearchDocument<T>) -> Result<()> {
        let mut this = &mut *self;
        this.tx.send(item).map_err(|e| {
            CallystoError::GeneralError(format!("Failed to send to index: `{}`", e.0.index_id))
        })
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if self.tx.len() > 0 {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if self.tx.len() > 0 {
            Poll::Pending
        } else {
            // TODO: Drop the task `data_sink`.
            Poll::Ready(Ok(()))
        }
    }
}
