use crate::errors::*;
use crate::runtime::async_con::Arc;
use crossbeam_channel::{Receiver, Sender};
use cuneiform_fields::arch::ArchPadding;
use futures::sink::{drain, With};
use futures::Future;
use futures::{Sink, SinkExt};
use futures_lite::FutureExt;
use nuclei::Task;
use pin_project_lite::pin_project;
use serde::Serialize;
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::{self, Connection, PgPool, Row};
use sqlx::{Executor, FromRow};
use std::collections::VecDeque;
use std::convert::Infallible;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::vec::Drain;
use tracing::{debug, trace, warn};
use url::Url;

#[derive(Debug)]
pub struct CPostgresRow<T, R> {
    pub query: String,
    pub row: T,
    _marker: PhantomData<R>,
}

impl<T, R> CPostgresRow<T, R>
where
    T: Send + Serialize + FromRow<'static, R>,
    R: Row,
{
    pub fn new(query: String, row: T) -> Self {
        Self {
            query: query,
            row: row,
            _marker: PhantomData::default(),
        }
    }
}

pin_project! {
    pub struct CPostgresSink<T, R>
    where
    T: Send + Serialize + FromRow<'static, R>,
    R: Row
    {
        client: Arc<sqlx::postgres::PgPool>,
        tx: ArchPadding<Sender<CPostgresRow<T, R>>>,
        buffer_size: usize,
        #[pin]
        data_sink: Task<()>
    }
}

impl<T, R> CPostgresSink<T, R>
where
    T: Serialize + Send + 'static,
{
    async fn setup_pg(dsn: &str, pool_size: u32) -> Result<PgPool> {
        let pgpool = PgPoolOptions::new()
            .max_connections(pool_size)
            .connect(dsn)
            .await?;
        Ok(pgpool)
    }
    pub fn new(pg_dsn: String, pool_size: u32, buffer_size: usize) -> Result<Self> {
        let pgpool =
            nuclei::block_on(async move { Self::setup_pg(&pg_dsn, pool_size).await.unwrap() });

        let (tx, rx) = crossbeam_channel::unbounded::<CPostgresRow<T, R>>();

        let (tx, rx) = (ArchPadding::new(tx), ArchPadding::new(rx));

        let client = Arc::new(pgpool);
        let data_sink = nuclei::spawn(async move {
            while let Ok(item) = rx.recv() {
                let row = sqlx::query_as::<_, T>(&item.query)
                    .execute(client)
                    .await
                    .unwrap();
                trace!("CPostgresSink - Ingestion status: {:#?}", row);
            }
        });

        Ok(Self {
            client,
            tx,
            buffer_size,
            data_sink,
        })
    }
}

impl<T, R> Sink<CPostgresRow<T, R>> for CPostgresSink<T, R>
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

    fn start_send(mut self: Pin<&mut Self>, item: CPostgresRow<T, R>) -> Result<()> {
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
            // let project = self.pr
            Poll::Ready(Ok(()))
        }
    }
}
