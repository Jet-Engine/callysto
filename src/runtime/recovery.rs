use crate::errors::*;
use crate::kafka::cconsumer::CConsumer;
use crate::prelude::{Context, ServiceState, Tables};
use crate::stores::store::Store;
use crate::types::collection::Collection;
use crate::types::service::Service;
use crate::types::table::CTable;
use async_trait::async_trait;
use futures::future::{join_all, BoxFuture};
use futures::{pin_mut, FutureExt};
use futures::{SinkExt, StreamExt};
use lever::prelude::LOTable;
use lever::sync::atomics::AtomicBox;
use nuclei::join_handle::JoinHandle;
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info};

type ConcurrentTables<State> = LOTable<String, Arc<CTable<State>>>;

pub struct RecoveryService<State>
where
    State: Clone + Send + Sync + 'static,
{
    app_name: String,
    state: State,
    tables: ConcurrentTables<State>,
    dependencies: Vec<Arc<dyn Service<State>>>,
}

impl<State> RecoveryService<State>
where
    State: Clone + Send + Sync + 'static,
{
    pub fn new(
        app_name: String,
        state: State,
        tables: ConcurrentTables<State>,
        dependencies: Vec<Arc<dyn Service<State>>>,
    ) -> Self {
        Self {
            app_name,
            state,
            tables,
            dependencies,
        }
    }

    async fn consume_changelogs(&self) -> Result<Vec<JoinHandle<()>>> {
        let tables = self.tables.clone();
        let tasks: Vec<JoinHandle<()>> = self
            .tables
            .iter()
            .map(|(_, table)| {
                let consumer = table.changelog_topic.consumer();
                (table, Arc::new(consumer))
            })
            .map(|(table, consumer)| {
                info!(
                    "Recovery is starting for changelog topic: `{}`",
                    table.changelog_topic_name()
                );
                let changelog_topic_name = table.changelog_topic_name();
                nuclei::spawn(async move {
                    info!(
                        "Recovery started for changelog topic: `{}`",
                        changelog_topic_name
                    );
                    let mut element_count = 0_usize;
                    let mut message_stream = consumer.cstream().ready_chunks(10);
                    while let Some(messages) = message_stream.next().await {
                        info!("Recovery received `{}` changelog objects.", messages.len());
                        let msgs: Vec<OwnedMessage> = messages
                            .iter()
                            .flat_map(|rm| match rm {
                                Some(bm) => Some(bm.to_owned()),
                                _ => {
                                    error!("Empty message received on recovery!");
                                    None
                                }
                            })
                            .collect();
                        element_count += msgs.len();
                        info!("Recovery wrote `{}` elements.", element_count);
                        table.apply_changelog_batch(msgs);
                    }
                    info!(
                        "Recovery finished for changelog topic: `{}`. `{}` elements written.",
                        table.changelog_topic_name(),
                        element_count
                    );
                })
            })
            .collect();

        Ok(tasks)
    }
}

#[async_trait]
impl<State> Service<State> for RecoveryService<State>
where
    State: Clone + Send + Sync + 'static,
{
    async fn call(&self, st: Context<State>) -> Result<State> {
        todo!()
    }

    async fn start(&self) -> Result<BoxFuture<'_, ()>> {
        let closure = async move {
            for x in &self.dependencies {
                info!(
                    "RecoveryService - {} - Dependencies are starting",
                    self.app_name
                );
                x.start().await;
            }

            info!(
                "Started Recovery Service - Consumer Group `{}`",
                self.app_name
            );

            'fallback: loop {
                info!("Launched Recovery Service worker.");
                self.service_state()
                    .await
                    .replace_with(|e| ServiceState::Running);
                'main: loop {
                    if self.stopped().await {
                        break 'main;
                    }
                    match self.consume_changelogs().await {
                        Ok(tasks) => {
                            let fut = join_all(tasks);
                            nuclei::spawn_blocking(move || nuclei::block_on(fut)).await;
                        }
                        Err(e) => {
                            error!("Changelog Service failed: {:?}", e);
                            self.crash().await;
                            break 'main;
                        }
                    }
                }

                if self.stopped().await {
                    break 'fallback;
                }
            }
        };

        Ok(closure.boxed())
    }

    async fn wait_until_stopped(&self) {
        todo!()
    }

    async fn state(&self) -> String {
        todo!()
    }

    async fn label(&self) -> String {
        format!("{}@{}", self.app_name, self.shortlabel().await)
    }

    async fn shortlabel(&self) -> String {
        String::from("recovery-service")
    }
}
