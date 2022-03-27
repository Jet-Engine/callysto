use std::borrow::Borrow;
use std::cell::{Cell, RefCell, UnsafeCell};
use std::collections::HashMap;
use std::default::Default;
use std::fmt::Alignment::Center;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures::future::join_all;
use http_types::headers::ToHeaderValues;
use http_types::Request;
use lever::prelude::{HOPTable, LOTable};
use lever::sync::atomics::AtomicBox;
use lightproc::prelude::RecoverableHandle;
use nuclei::join_handle::JoinHandle;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, MessageStream, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use rdkafka::ClientConfig;
use tracing::{error, info};
use tracing_subscriber::{self, fmt, EnvFilter};
use url::Url;

use crate::config::Config;
use crate::errors::Result as CResult;
use crate::kafka::{ctopic::*, runtime::NucleiRuntime};
use crate::runtime::recovery::RecoveryService;
use crate::runtime::web::Web;
use crate::table::CTable;
use crate::types::agent::{Agent, CAgent};
use crate::types::context::*;
use crate::types::cronjob::CronJob;
use crate::types::route::{Route, Router};
use crate::types::service::Service;
use crate::types::table_agent::{CTableAgent, TableAgent, Tables};
use crate::types::task::Task;

// TODO: not sure static dispatch is better here. Check on using State: 'static.

///
/// Struct that will help architecting your application.
///
/// This contains application's runner in addition to the function modules that will design
/// your application.
pub struct Callysto<State>
where
    State: Clone + Send + Sync + 'static,
{
    app_name: String,
    state: State,
    storage_url: Option<Url>,
    brokers: String,
    config: Config,
    stubs: Arc<AtomicUsize>,
    stub_lookup: LOTable<usize, String>,
    tasks: LOTable<usize, Arc<dyn Task<State>>>,
    timers: LOTable<usize, Arc<dyn Task<State>>>,
    cronjobs: LOTable<usize, Arc<CronJob<State>>>,
    services: LOTable<usize, Arc<dyn Service<State>>>,
    agents: LOTable<usize, Arc<dyn Agent<State>>>,
    tables: LOTable<String, Arc<CTable<State>>>,
    table_agents: LOTable<usize, Arc<dyn TableAgent<State>>>,
    routes: LOTable<String, Arc<dyn Router<State>>>,
}

impl Callysto<()> {
    ///
    /// Creates an application without shared state between it's modules.
    ///
    /// Modules are going to use `()` as state which gets optimized away.
    ///
    /// # Example
    /// ```rust
    /// use callysto::prelude::*;
    ///
    /// fn main() {
    ///     let mut app = Callysto::new();
    ///     app.with_name("example-app");
    /// }
    /// ```
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
    ///
    /// Create application with given `State`.
    ///
    /// This state will be shared in whole application's scope. It's instance specific and other
    /// workers that ran separately will not see this shared state. In other words, this state is not distributed.
    ///
    /// This state will be injected via [Context] to modules.
    ///
    /// # Example
    /// ```rust
    /// use callysto::prelude::*;
    ///
    /// // Shared State
    /// #[derive(Clone)]
    /// struct State {
    ///    name: String,
    /// }
    ///
    /// fn main() {
    ///     let state = State { name: "Callysto".into() };
    ///     let mut app = Callysto::with_state(state);
    ///     app.with_name("stateful-app");
    /// }
    /// ```
    pub fn with_state(state: State) -> Self {
        Self {
            app_name: "callysto-app".to_owned(),
            state,
            storage_url: None,
            stubs: Arc::new(AtomicUsize::default()),
            stub_lookup: LOTable::default(),
            brokers: "localhost:9092".to_owned(),
            config: Config::default(),
            tasks: LOTable::default(),
            timers: LOTable::default(),
            cronjobs: LOTable::default(),
            services: LOTable::default(),
            agents: LOTable::default(),
            tables: LOTable::default(),
            table_agents: LOTable::default(),
            routes: LOTable::default(),
        }
    }

    ///
    /// Durable/Non durable storage to run your application with.
    /// This storage will be used for all storage needs that application requires.
    /// Given storage driver will be used for all distributed tables of this application instance.
    /// You application workers can use different driver backends across your deployment.
    /// e.g. instance 1 can use rocksdb backend, but your instance 2 can use aerospike.
    /// Driver's are detached from your application code in that regard.
    ///
    /// Storage backends can be enabled with feature flag.
    /// At the moment you can't use more than one storage backend for the same instance.
    /// But this will be possible in future.
    ///
    /// ### Example storage url schemes are:
    /// 1. `rocksdb:///home/callysto/datadir`
    /// 2. `aerospike:///home/callysto/datadir`
    /// 3. `inmemory:///home/callysto/datadir`
    ///
    /// Mind that `inmemory` backend doesn't supply full durability across crashes or instance restarts.
    ///
    /// # Example
    /// ```rust
    /// use callysto::prelude::*;
    ///
    /// fn main() {
    ///     let mut app = Callysto::new();
    ///     app.with_storage("rocksdb:///home/callysto/datadir");
    /// }
    /// ```
    pub fn with_storage<T>(&mut self, url: T) -> &mut Self
    where
        T: AsRef<str>,
    {
        let url = Url::parse(url.as_ref()).expect("Storage backend url parsing failed.");
        self.storage_url = Some(url);
        self
    }

    ///
    /// By default `callysto-app` is used internally as application name.
    /// If you want to change this you can use this method.
    /// This method will change also consumer group names and changelog topic names.
    /// Changing this will create different changelog topics for your tables.
    /// Previous changelog topics won't be cleaned.
    ///
    /// # Example
    /// ```rust
    /// use callysto::prelude::*;
    ///
    /// fn main() {
    ///     let mut app = Callysto::new();
    ///     app.with_name("amazing-service-of-mine");
    /// }
    /// ```
    pub fn with_name<T: AsRef<str>>(&mut self, name: T) -> &mut Self {
        self.app_name = name.as_ref().to_string();
        self
    }

    ///
    /// Kafka brokers to connect to.
    /// By default application will connect to `localhost:9092`.
    ///
    /// For multiple brokers, please use comma separated broker list.
    ///
    /// # Example
    /// ```rust
    /// use callysto::prelude::*;
    ///
    /// fn main() {
    ///     let mut app = Callysto::new();
    ///     app.with_brokers("kafka1:9092,kafka2:9092,kafka3:9092");
    /// }
    /// ```
    pub fn with_brokers<T: AsRef<str>>(&mut self, brokers: T) -> &mut Self {
        self.brokers = brokers.as_ref().to_string();
        self
    }

    ///
    /// Callysto apps composed of modules. Task is one of these modules.
    /// Task is used for one-off tasks that will start when your application starts.
    ///
    /// As soon as your application worker is running, tasks will fire.
    /// If you want to do something at periodic intervals, use [Timer](Callysto::timer).
    pub fn task(&self, t: impl Task<State>) -> &Self {
        let stub = self.stubs.fetch_add(1, Ordering::AcqRel);
        self.tasks.insert(stub, Arc::new(t));
        self
    }

    ///
    /// A timer is a task that executes in every given interval.
    /// After application worker is ready, timer will start working and execute the given task periodically.
    pub fn timer(&self, interval: f64, t: impl Task<State>) -> &Self {
        let stub = self.stubs.fetch_add(1, Ordering::AcqRel);
        self.timers.insert(stub, Arc::new(t));
        self
    }

    pub fn agent<T: AsRef<str>, F, Fut>(&self, name: T, topic: CTopic, clo: F) -> &Self
    where
        F: Send + Sync + 'static + Fn(Option<OwnedMessage>, Context<State>) -> Fut,
        Fut: Future<Output = CResult<()>> + Send + 'static,
    {
        let stub = self.stubs.fetch_add(1, Ordering::AcqRel);
        let agent = CAgent::new(
            clo,
            name.as_ref().to_string(),
            self.app_name.clone(),
            self.state.clone(),
            topic,
            Vec::default(),
        );
        self.agents.insert(stub, Arc::new(agent));
        self
    }

    pub fn table_agent<T: AsRef<str>, F, Fut>(
        &self,
        name: T,
        topic: CTopic,
        mut tables: HashMap<String, CTable<State>>,
        clo: F,
    ) -> &Self
    where
        F: Send + Sync + 'static + Fn(Option<OwnedMessage>, Tables<State>, Context<State>) -> Fut,
        Fut: Future<Output = CResult<()>> + Send + 'static,
    {
        let stub = self.stubs.fetch_add(1, Ordering::AcqRel);
        let table_agent = CTableAgent::new(
            clo,
            name.as_ref().to_string(),
            self.app_name.clone(),
            self.state.clone(),
            topic,
            tables,
            Vec::default(),
        );
        self.table_agents.insert(stub, Arc::new(table_agent));
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
        let cc = self.build_client_config();
        CTopic::new(topic, cc)
    }

    // TODO: page method to serve
    pub fn page<T: AsRef<str>, F, Fut>(&self, at: T, route: F) -> &Self
    where
        F: Send + Sync + 'static + Fn(http_types::Request, Context<State>) -> Fut,
        Fut: Future<Output = http_types::Result<http_types::Response>> + Send + 'static,
    {
        let stub = self.stubs.fetch_add(1, Ordering::AcqRel);
        let route = Route::new(
            route,
            self.state.clone(),
            at.as_ref().to_string(),
            self.app_name.clone(),
        );
        self.routes.insert(at.as_ref().to_string(), Arc::new(route));
        self
    }

    fn build_client_config(&self) -> ClientConfig {
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
                "statistics.interval.ms",
                format!("{}", self.config.statistics_interval_ms),
            )
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
        cc
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

    pub fn table<T: AsRef<str>>(&self, table_name: T) -> CTable<State> {
        if self.storage_url.is_none() {
            panic!("Tables can't be used without storage backend. Bailing...");
        }
        let stub = self.stubs.fetch_add(1, Ordering::AcqRel);
        let table = CTable::new(
            self.app_name.clone(),
            self.storage_url.clone().unwrap(),
            table_name.as_ref().into(),
            self.config.clone(),
            self.build_client_config(),
        )
        .expect("Table build failed.");

        self.tables
            .insert(table_name.as_ref().into(), Arc::new(table.clone()));
        table
    }

    // TODO: table_route method to give data based on page slug

    fn background_workers(&self) -> CResult<()> {
        // Add Recovery Service
        self.service(RecoveryService::new(
            self.app_name.clone(),
            self.state.clone(),
            self.tables.clone(),
            self.tables
                .values()
                .map(|e| e as Arc<dyn Service<State>>)
                .collect::<Vec<_>>(),
        ));

        // Add Web Service
        self.service(Web::new(
            self.app_name.clone(),
            self.state.clone(),
            self.routes.values().into_iter().collect(),
            vec![],
        ));

        Ok(())
    }

    pub fn run(self) {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .init();

        // Load all background workers
        self.background_workers();

        let mut workers: Vec<JoinHandle<()>> = Vec::with_capacity(
            self.agents.len()
                + self.services.len()
                + self.table_agents.len()
                + self.cronjobs.len()
                + self.tasks.len()
                + self.timers.len(),
        );

        let mut agents: Vec<JoinHandle<()>> = self
            .agents
            .iter()
            .map(|(aid, agent)| {
                info!("Starting Agent with ID: {}", aid);
                // TODO: Recovery should be here.
                nuclei::spawn(async move {
                    match agent.start().await {
                        Ok(dep) => dep.await,
                        _ => panic!("Error occurred on start of Agent with ID: {}.", aid),
                    }

                    agent.after_start().await;
                })
            })
            .collect();

        let table_agents: Vec<JoinHandle<()>> = self
            .table_agents
            .iter()
            .map(|(aid, agent)| {
                info!("Starting Table Agent with ID: {}", aid);
                // TODO: Recovery should be here.
                nuclei::spawn(async move {
                    match agent.start().await {
                        Ok(dep) => dep.await,
                        _ => panic!("Error occurred on start of Table Agent with ID: {}.", aid),
                    }

                    agent.after_start().await;
                })
            })
            .collect();

        let services: Vec<JoinHandle<()>> = self
            .services
            .iter()
            .map(|(sid, service)| {
                info!("Starting Service with ID: {}", sid);
                // TODO: Recovery should be here.
                nuclei::spawn(async move {
                    match service.start().await {
                        Ok(dep) => dep.await,
                        _ => panic!("Error occurred on start of Service with ID: {}.", sid),
                    }

                    service.after_start().await;
                })
            })
            .collect();

        workers.extend(agents);
        workers.extend(table_agents);
        workers.extend(services);

        nuclei::block_on(join_all(workers));
    }
}
