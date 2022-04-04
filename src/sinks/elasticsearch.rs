use std::pin::Pin;
use std::task::{Context, Poll};
use elasticsearch::Elasticsearch;
use elasticsearch::http::transport::Transport;
use futures::Sink;
use serde::Serialize;
use url::Url;
use crate::errors::*;


pub struct CElasticSearchSink {

}

impl CElasticSearchSink {
    pub fn new(elastic_url: Url) -> Result<Self> {
        let transport = Transport::single_node(elastic_url.as_ref())?;
        let client = Elasticsearch::new(transport);
        todo!()
    }
}

impl<T> Sink<T> for CElasticSearchSink
where
    T: Serialize
{
    type Error = CallystoError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        todo!()
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<()> {
        todo!()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        todo!()
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        todo!()
    }
}