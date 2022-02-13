use crate::definitions::*;
use crate::table::CTable;
use crate::kafka::{CTopic, BastionRuntime};
use lever::sync::atomics::AtomicBox;
use std::sync::Arc;
use lever::prelude::{LOTable, HOPTable};
use std::sync::atomic::{AtomicUsize, Ordering};
use crate::prelude::{CronJob, Config};
use rdkafka::ClientConfig;
use rdkafka::consumer::{StreamConsumer, Consumer};

pub struct Callysto<Store>
where
    Store: 'static
{
    app_name: String,
    storage: Store,
    brokers: String,
    config: Config,
    stubs: Arc<AtomicUsize>,
    tasks: LOTable<usize, Arc<dyn Task<Store>>>,
    timers: LOTable<usize, Arc<dyn Task<Store>>>,
    cronjobs: LOTable<usize, Arc<CronJob<Store>>>,
    services: LOTable<usize, Arc<dyn Service<Store>>>,
    agents: LOTable<usize, Arc<dyn Agent<Store>>>,
}

impl Callysto<()> {
    #[must_use]
    pub fn new() -> Self {
        Self::with_storage(())
    }
}

impl Default for Callysto<()> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Store> Callysto<Store>
where
    Store: Clone + Send + Sync + 'static
{
    pub fn with_storage(storage: Store) -> Self {
        Self {
            app_name: "callysto-app".to_owned(),
            storage,
            stubs: Arc::new(AtomicUsize::default()),
            brokers: "localhost:9092".to_owned(),
            config: Config::default(),
            tasks: LOTable::default(),
            timers: LOTable::default(),
            cronjobs: LOTable::default(),
            services: LOTable::default(),
            agents: LOTable::default()
        }
    }

    pub fn with_name<T: AsRef<str>>(&mut self, name: T) -> &mut Self {
        self.app_name = name.as_ref().to_string();
        self
    }

    pub fn with_brokers<T: AsRef<str>>(&mut self, brokers: T) -> &mut Self {
        self.brokers = brokers.as_ref().to_string();
        self
    }

    pub fn task(&self, t: impl Task<Store>) -> &Self {
        let stub = self.stubs.fetch_add(1, Ordering::AcqRel);
        self.tasks.insert(stub, Arc::new(t));
        self
    }

    pub fn timer(&self, t: impl Task<Store>) -> &Self {
        let stub = self.stubs.fetch_add(1, Ordering::AcqRel);
        self.timers.insert(stub, Arc::new(t));
        self
    }

    pub fn agent(&self, s: impl Agent<Store>) -> &Self
    {
        let stub = self.stubs.fetch_add(1, Ordering::AcqRel);
        self.agents.insert(stub, Arc::new(s));
        self
    }

    pub fn service(&self, s: impl Service<Store>) -> &Self {
        let stub = self.stubs.fetch_add(1, Ordering::AcqRel);
        self.services.insert(stub, Arc::new(s));
        self
    }

    pub fn crontab<C: AsRef<str>>(&self, cron_expr: C, t: impl Task<Store>) -> &Self {
        let stub = self.stubs.fetch_add(1, Ordering::AcqRel);
        let cron_job = Arc::new(CronJob::new(cron_expr, t));
        self.cronjobs.insert(stub, cron_job);
        self
    }

    pub fn topic<T>(&self, topic: T) -> CTopic
    where
        T: AsRef<str>
    {

        let mut cc = ClientConfig::new();

        cc
            .set("bootstrap.servers", &*self.brokers)
            .set("enable.auto.commit", format!("{}", self.config.enable_auto_commit))
            .set("auto.offset.reset", format!("{}", self.config.auto_offset_reset))
            .set("auto.commit.interval.ms", format!("{}", self.config.auto_commit_interval_ms))
            .set("enable.auto.offset.store", format!("{}", self.config.enable_auto_offset_store))
            .set("max.poll.interval.ms", format!("{}", self.config.max_poll_interval_ms))
            .set("max.partition.fetch.bytes", format!("{}", self.config.max_partition_fetch_bytes))
            .set("fetch.max.wait.ms", format!("{}", self.config.fetch_max_wait_ms))
            .set("request.timeout.ms", format!("{}", self.config.request_timeout_ms))
            .set("check.crcs", format!("{}", self.config.check_crcs))
            .set("session.timeout.ms", format!("{}", self.config.session_timeout_ms))
            .set("heartbeat.interval.ms", format!("{}", self.config.heartbeat_interval_ms))
            .set("isolation.level", format!("{}", self.config.isolation_level))
            // Consumer group ID
            .set("group.id", self.app_name.as_str());

        // Security settings
        let cc = cc
            .set("security.protocol", format!("{}", self.config.security_protocol));

        use crate::enums::SecurityProtocol::*;
        let cc = match self.config.security_protocol {
            Ssl => {
                // SSL context is passed down with these arguments.
                self.build_ssl_context(cc);
                cc
            },
            SaslPlaintext => {
                // Only SASL context build is needed.
                self.build_sasl_context(cc);
                cc
            },
            SaslSsl => {
                // Build both contexts with available arguments.
                self.build_sasl_context(cc);
                self.build_ssl_context(cc);
                cc
            }
            _ => cc,
        };


        let consumer: StreamConsumer<_, BastionRuntime> = cc
            .create()
            .expect("Consumer creation failed");
        consumer.subscribe(&[topic.as_ref()]).unwrap();
        todo!()
    }

    fn build_sasl_context(&self, mut cc: &mut ClientConfig) {
        self.config.sasl_mechanism.clone().map(|e| {
            cc.set("sasl.mechanism", format!("{}", e))
        });

        self.config.sasl_username.clone().map(|e| {
            cc.set("sasl.username", e)
        });

        self.config.sasl_password.clone().map(|e| {
            cc.set("sasl.password", e)
        });
    }

    fn build_ssl_context(&self, mut cc: &mut ClientConfig) {
        self.config.ssl_certificate_location.clone().map(|e| {
            cc.set("ssl.certificate.location", e)
        });

        self.config.ssl_ca_location.clone().map(|e| {
            cc.set("ssl.ca.location", e)
        });

        self.config.ssl_key_location.clone().map(|e| {
            cc.set("ssl.key.location", e)
        });

        self.config.ssl_key_password.clone().map(|e| {
            cc.set("ssl.key.password", e)
        });

        self.config.ssl_endpoint_identification_algorithm.clone().map(|e| {
            cc.set("ssl.endpoint.identification.algorithm", format!("{}", e))
        });
    }

    pub fn table(&self) -> CTable {
        todo!()
    }

    // TODO: page method to serve
    // TODO: table_route method to give data based on page slug
}