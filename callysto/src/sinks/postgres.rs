use crate::errors::*;
use crate::runtime::async_con::Arc;
use crossbeam_channel::{Receiver, Sender};
use cuneiform_fields::arch::ArchPadding;
use deadpool_postgres::tokio_postgres::types::ToSql;
use deadpool_postgres::{Client, Config, Pool, PoolError, Runtime};
use deadpool_postgres::{Manager, ManagerConfig, RecyclingMethod};
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
use std::str::FromStr;
use std::task::{Context, Poll};
use std::vec::Drain;
use tracing::{debug, trace, warn};
use url::Url;

#[derive(Debug)]
pub struct CPostgresRow<T: ToSql + Sync + 'static + Send> {
    pub query: String,
    pub args: Vec<T>,
}

impl<T> CPostgresRow<T>
where
    T: Send + ToSql + Sync + 'static,
{
    pub fn new(query: String, args: Vec<T>) -> Self {
        Self { query, args }
    }
}

pin_project! {
    pub struct CPostgresSink<T>
    where
    T: ToSql,
    T: Sync,
    T: Send,
    T: 'static
    {
        client: Arc<deadpool_postgres::Pool>,
        tx: ArchPadding<Sender<CPostgresRow<T>>>,
        buffer_size: usize,
        #[pin]
        data_sink: Task<()>
    }
}

impl<T> CPostgresSink<T>
where
    T: ToSql + Sync + 'static + Send,
{
    async fn setup_pg(dsn: &str, tls: bool, pool_size: usize) -> Result<Pool> {
        // TODO(ansrivas): Currently only NoTls is supported, will add it later.
        let pg_config = deadpool_postgres::tokio_postgres::Config::from_str(dsn)?;
        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };

        let mgr = Manager::from_config(
            pg_config,
            deadpool_postgres::tokio_postgres::NoTls,
            mgr_config,
        );
        let pool = Pool::builder(mgr)
            .max_size(pool_size)
            .build()
            .map_err(|e| CallystoError::GeneralError(e.to_string()))?;
        Ok(pool)
    }

    pub fn new<S: Into<String>>(pg_dsn: S, pool_size: usize, buffer_size: usize) -> Result<Self> {
        let pgpool = nuclei::block_on(async move {
            Self::setup_pg(&pg_dsn.into(), false, pool_size)
                .await
                .unwrap()
        });

        info!("using clone version of postgres library sink");

        let (tx, rx) = crossbeam_channel::unbounded::<CPostgresRow<T>>();
        let (tx, rx) = (ArchPadding::new(tx), ArchPadding::new(rx));

        let inner_client = pgpool.clone();
        let client = Arc::new(pgpool);
        let data_sink = nuclei::spawn(async move {
            while let Ok(item) = rx.recv() {
                let mut client = inner_client.get().await.unwrap_or_else(|e| {
                    error!("{:?}", e);
                });
                let stmt = client
                    .prepare_cached(&item.query)
                    .await
                    .unwrap_or_else(|e| {
                        error!("{:?}", e);
                    });
                info!("statement: {:?}", stmt);
                let rows = client
                    .query_raw(&stmt, &item.args)
                    .await
                    .unwrap_or_else(|e| {
                        error!("{:?}", e);
                    });
                info!("rows: {:?}", rows);
                info!("CPostgresSink - Ingestion status:");
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

impl<T> Sink<CPostgresRow<T>> for CPostgresSink<T>
where
    T: ToSql + Sync + 'static + Send,
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

    fn start_send(mut self: Pin<&mut Self>, item: CPostgresRow<T>) -> Result<()> {
        let mut this = &mut *self;
        this.tx
            .send(item)
            .map_err(|e| CallystoError::GeneralError(format!("Failed to send to db: `{}`", e)))
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
