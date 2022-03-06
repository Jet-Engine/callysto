use std::default::Default;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bastion::spawn;
use futures::future::join_all;
use lever::prelude::{HOPTable, LOTable};
use lever::sync::atomics::AtomicBox;
use lightproc::prelude::RecoverableHandle;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, MessageStream, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use rdkafka::ClientConfig;
use tracing::{error, info};
use tracing_subscriber::{self, fmt, EnvFilter};
use url::Url;

use crate::definitions::*;
use crate::kafka::{ctopic::*, runtime::BastionRuntime};
use crate::prelude::{Config, CronJob};
use crate::service::Service;
use crate::table::CTable;

pub struct Callysto<State>
where
    State: 'static,
{
    app_name: String,
    state: State,
    storage_url: Option<Url>,
    brokers: String,
    config: Config,
    stubs: Arc<AtomicUsize>,
    tasks: LOTable<usize, Arc<dyn Task<State>>>,
    timers: LOTable<usize, Arc<dyn Task<State>>>,
    cronjobs: LOTable<usize, Arc<CronJob<State>>>,
    services: LOTable<usize, Arc<dyn Service<State>>>,
    agents: LOTable<usize, Arc<dyn Agent<State>>>,
    topics: LOTable<usize, CTopic>,
}

impl Callysto<()> {
    #[must_use]
    pub fn new() -> Self {
        Self::with_state(())
    }
}

impl Default for Callysto<()> {
    fn default() -> Self {
        Self::new()
    }
}

impl<State> Callysto<State>
where
    State: Clone + Send + Sync + 'static,
{
    pub fn with_state(state: State) -> Self {
        Self {
            app_name: "callysto-app".to_owned(),
            state,
            storage_url: None,
            stubs: Arc::new(AtomicUsize::default()),
            brokers: "localhost:9092".to_owned(),
            config: Config::default(),
            tasks: LOTable::default(),
            timers: LOTable::default(),
            cronjobs: LOTable::default(),
            services: LOTable::default(),
            agents: LOTable::default(),
            topics: LOTable::default(),
        }
    }

    pub fn with_storage<T>(&mut self, url: T) -> &mut Self
    where
        T: AsRef<str>,
    {
        let url = Url::parse(url.as_ref()).expect("Storage backend url parsing failed.");
        self.storage_url = Some(url);
        self
    }

    pub fn with_name<T: AsRef<str>>(&mut self, name: T) -> &mut Self {
        self.app_name = name.as_ref().to_string();
        self
    }

    pub fn with_brokers<T: AsRef<str>>(&mut self, brokers: T) -> &mut Self {
        self.brokers = brokers.as_ref().to_string();
        self
    }

    pub fn task(&self, t: impl Task<State>) -> &Self {
        let stub = self.stubs.fetch_add(1, Ordering::AcqRel);
        self.tasks.insert(stub, Arc::new(t));
        self
    }

    pub fn timer(&self, t: impl Task<State>) -> &Self {
        let stub = self.stubs.fetch_add(1, Ordering::AcqRel);
        self.timers.insert(stub, Arc::new(t));
        self
    }

    pub fn agent(&self, topic: CTopic, s: impl Agent<State>) -> &Self {
        let stub = self.stubs.fetch_add(1, Ordering::AcqRel);
        self.agents.insert(stub, Arc::new(s));
        self.topics.insert(stub, topic);
        self
    }

    pub fn service(&self, s: impl Service<State>) -> &Self {
        let stub = self.stubs.fetch_add(1, Ordering::AcqRel);
        self.services.insert(stub, Arc::new(s));
        self
    }

    pub fn crontab<C: AsRef<str>>(&self, cron_expr: C, t: impl Task<State>) -> &Self {
        let stub = self.stubs.fetch_add(1, Ordering::AcqRel);
        let cron_job = Arc::new(CronJob::new(cron_expr, t));
        self.cronjobs.insert(stub, cron_job);
        self
    }

    pub fn topic<T>(&self, topic: T) -> CTopic
    where
        T: AsRef<str>,
    {
        let mut cc = ClientConfig::new();

        cc.set("bootstrap.servers", &*self.brokers)
            .set(
                "enable.auto.commit",
                format!("{}", self.config.enable_auto_commit),
            )
            .set(
                "auto.offset.reset",
                format!("{}", self.config.auto_offset_reset),
            )
            .set(
                "auto.commit.interval.ms",
                format!("{}", self.config.auto_commit_interval_ms),
            )
            .set(
                "enable.auto.offset.store",
                format!("{}", self.config.enable_auto_offset_store),
            )
            .set(
                "max.poll.interval.ms",
                format!("{}", self.config.max_poll_interval_ms),
            )
            .set(
                "max.partition.fetch.bytes",
                format!("{}", self.config.max_partition_fetch_bytes),
            )
            .set(
                "fetch.wait.max.ms",
                format!("{}", self.config.fetch_max_wait_ms),
            )
            .set(
                "request.timeout.ms",
                format!("{}", self.config.request_timeout_ms),
            )
            .set("check.crcs", format!("{}", self.config.check_crcs))
            .set(
                "session.timeout.ms",
                format!("{}", self.config.session_timeout_ms),
            )
            .set(
                "heartbeat.interval.ms",
                format!("{}", self.config.heartbeat_interval_ms),
            )
            .set(
                "isolation.level",
                format!("{}", self.config.isolation_level),
            )
            // Consumer group ID
            .set("group.id", self.app_name.as_str());

        // Security settings
        cc.set(
            "security.protocol",
            format!("{}", self.config.security_protocol),
        );

        use crate::kafka::enums::SecurityProtocol::*;
        let cc = match self.config.security_protocol {
            Ssl => {
                // SSL context is passed down with these arguments.
                self.build_ssl_context(cc)
            }
            SaslPlaintext => {
                // Only SASL context build is needed.
                self.build_sasl_context(cc)
            }
            SaslSsl => {
                // Build both contexts with available arguments.
                let cc = self.build_sasl_context(cc);
                self.build_ssl_context(cc)
            }
            _ => cc,
        };

        CTopic::new(topic, cc)
    }

    fn build_sasl_context(&self, mut cc: ClientConfig) -> ClientConfig {
        self.config
            .sasl_mechanism
            .clone()
            .map(|e| cc.set("sasl.mechanism", format!("{}", e)));

        self.config
            .sasl_username
            .clone()
            .map(|e| cc.set("sasl.username", e));

        self.config
            .sasl_password
            .clone()
            .map(|e| cc.set("sasl.password", e));

        cc
    }

    fn build_ssl_context(&self, mut cc: ClientConfig) -> ClientConfig {
        self.config
            .ssl_certificate_location
            .clone()
            .map(|e| cc.set("ssl.certificate.location", e));

        self.config
            .ssl_ca_location
            .clone()
            .map(|e| cc.set("ssl.ca.location", e));

        self.config
            .ssl_key_location
            .clone()
            .map(|e| cc.set("ssl.key.location", e));

        self.config
            .ssl_key_password
            .clone()
            .map(|e| cc.set("ssl.key.password", e));

        self.config
            .ssl_endpoint_identification_algorithm
            .clone()
            .map(|e| cc.set("ssl.endpoint.identification.algorithm", format!("{}", e)));

        cc
    }

    pub fn table(&self, table_name: String) -> CTable<State> {
        todo!()
    }

    // TODO: page method to serve
    // TODO: table_route method to give data based on page slug

    pub fn run(self) {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .init();

        let agents: Vec<RecoverableHandle<()>> = self
            .agents
            .iter()
            .zip(self.topics.iter())
            .map(|((_aid, agent), (_tid, topic))| {
                let state = self.state.clone();
                let consumer_group_name = self.app_name.clone();

                bastion::executor::blocking(async move {
                    let consumer = topic.consumer();
                    info!(
                        "Started - Consumer Group `{}` - Topic `{}`",
                        consumer_group_name,
                        topic.topic_name()
                    );
                    loop {
                        let state = state.clone();
                        let message = consumer.recv().await;
                        let context = Context::new(state);
                        let _slow_drop = agent.call(message, context).await.unwrap();
                    }
                })
            })
            .collect();

        bastion::executor::run(join_all(agents));
    }
}
