use std::borrow::Borrow;
use std::cell::{Cell, RefCell, UnsafeCell};
use std::collections::HashMap;
use std::default::Default;
use std::fmt::Alignment::Center;
use std::future::Future;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use http_types::headers::ToHeaderValues;
use http_types::Request;
use lever::prelude::{HOPTable, LOTable};
use lever::sync::atomics::AtomicBox;
use nuclei::Task as AsyncTask;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, MessageStream, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use rdkafka::ClientConfig;
use tracing::{error, info};
use url::Url;

use crate::config::Config;
use crate::errors::Result as CResult;
use crate::kafka::{ctopic::*, runtime::NucleiRuntime};
use crate::prelude::{CTask, CTimer};
use crate::runtime::recovery::RecoveryService;
use crate::runtime::web::Web;
use crate::types::agent::{Agent, CAgent};
use crate::types::context::*;
use crate::types::cronjob::CronJob;
use crate::types::route::{Route, Router};
use crate::types::service::Service;
use crate::types::table::CTable;
use crate::types::table_agent::{CTableAgent, TableAgent, Tables};
use crate::types::task::Task;

use crate::prelude::*;
use futures::Stream;
use futures_timer::Delay;
use rdkafka::producer::FutureProducer;
use crate::types::flows::{CFlow, CSource, Flow};

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
    flows: LOTable<usize, Arc<dyn Service<State>>>,
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
    /// let mut app = Callysto::new();
    /// app.with_name("example-app");
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
    /// let state = State { name: "Callysto".into() };
    /// let mut app = Callysto::with_state(state);
    /// app.with_name("stateful-app");
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
            flows: LOTable::default(),
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
    /// let mut app = Callysto::new();
    /// app.with_storage("rocksdb:///home/callysto/datadir");
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
    /// Build the application with the given config
    pub fn with_config(&mut self, config: Config) -> &mut Self {
        self.config = config;
        self
    }

    ///
    /// Set state on demand for global wide access.
    pub fn set_state(&mut self, state: State) -> &mut Self {
        self.state = state;
        self
    }

    ///
    /// Get state on demand for global wide access.
    pub fn get_state(&mut self) -> State {
        self.state.clone()
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
    /// let mut app = Callysto::new();
    /// app.with_name("amazing-service-of-mine");
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
    /// let mut app = Callysto::new();
    /// app.with_brokers("kafka1:9092,kafka2:9092,kafka3:9092");
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
    pub fn task<F, Fut>(&self, clo: F) -> &Self
    where
        F: Send + Sync + 'static + Fn(Context<State>) -> Fut,
        Fut: Future<Output = CResult<()>> + Send + 'static,
    {
        let stub = self.stubs.fetch_add(1, Ordering::AcqRel);
        let task = CTask::new(clo, self.app_name.clone(), self.state.clone());
        self.tasks.insert(stub, Arc::new(task));
        self
    }

    ///
    /// A timer is a task that executes in every given interval.
    /// After application worker is ready, timer will start working and execute the given task periodically.
    pub fn timer<F, Fut>(&self, interval_seconds: f64, clo: F) -> &Self
    where
        F: Send + Sync + 'static + Fn(Context<State>) -> Fut,
        Fut: Future<Output = CResult<()>> + Send + 'static,
    {
        let stub = self.stubs.fetch_add(1, Ordering::AcqRel);
        let task = CTimer::new(
            clo,
            self.app_name.clone(),
            interval_seconds,
            self.state.clone(),
        );
        self.timers.insert(stub, Arc::new(task));
        self
    }

    ///
    /// An Agent is an service that consumes a topic as stream and processes it internally.
    /// There is no
    ///
    /// # Example
    /// ```rust
    /// use callysto::prelude::*;
    ///
    /// let mut app = Callysto::new();
    /// app.with_name("amazing-service-of-mine");
    /// ```
    pub fn agent<T: AsRef<str>, F, Fut>(&self, name: T, topic: CTopic, clo: F) -> &Self
    where
        F: Send + Sync + 'static + Fn(CStream, Context<State>) -> Fut,
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

    /// Helper to define custom service that skips or uses global shared state.
    pub fn service(&self, s: impl Service<State>) -> &Self {
        let stub = self.stubs.fetch_add(1, Ordering::AcqRel);
        self.services.insert(stub, Arc::new(s));
        self
    }

    /// Helper to define sources from streams
    pub fn source<S: Stream + Clone + Send + Sync + 'static>(&self, stream: S) -> CSource<S> {
        CSource::new(stream)
    }

    /// Helper to define flow that skips or uses global shared state.
    pub fn flow<T: AsRef<str>, F, S, R, Fut>(&self, name: T, stream: CSource<S>, clo: F) -> &Self
    where
        R: 'static + Send,
        S: Stream + Clone + Send + Sync + 'static,
        State: Clone + Send + Sync + 'static,
        F: Send + Sync + 'static + Fn(CSource<S>, Context<State>) -> Fut,
        Fut: Future<Output = CResult<R>> + Send + 'static,
    {
        let stub = self.stubs.fetch_add(1, Ordering::AcqRel);
        let flow = CFlow::new(
            stream,
            clo,
            self.app_name.clone(),
            name.as_ref().to_string(),
            self.state.clone(),
            Vec::default(),
        );
        self.flows.insert(stub, Arc::new(flow));
        self
    }

    /// Helper to define stateful service that uses global application level state.
    pub fn stateful_service<T, F, Fut>(&self, name: T, clo: F, dependencies: Vec<Arc<dyn Service<State>>>) -> &Self
    where
        T: AsRef<str>,
        F: Send + Sync + 'static + Fn(Context<State>) -> Fut,
        Fut: Future<Output = CResult<State>> + Send + 'static,
    {
        let stub = self.stubs.fetch_add(1, Ordering::AcqRel);
        let service = CService::new(
            name,
            clo,
            self.state.clone(),
            Vec::default(),
        );
        self.services.insert(stub, Arc::new(service));
        self
    }

    pub fn crontab<C: AsRef<str>>(&self, cron_expr: C, t: impl Task<State>) -> &Self {
        let stub = self.stubs.fetch_add(1, Ordering::AcqRel);
        let cron_job = Arc::new(CronJob::new(cron_expr, t));
        self.cronjobs.insert(stub, cron_job);
        self
    }

    ///
    /// Use default application client configuration for the topic
    pub fn topic<T>(&self, topic: T) -> CTopic
    where
        T: AsRef<str>,
    {
        let cc = self.build_client_config();
        CTopic::new(topic, cc)
    }

    // TODO: We need to allow this to be passed per agent.
    ///
    /// Allow passing custom client configuration for a specific topic
    pub fn topic_with_config<T>(
        &self,
        topic: T,
        client_config: Option<HashMap<String, String>>,
    ) -> CTopic
    where
        T: AsRef<str>,
    {
        let mut cc = self.build_client_config();
        if let Some(icc) = client_config {
            icc.iter().for_each(|(k, v)| {
                cc.set(k, v);
            });
        }
        CTopic::new(topic, cc)
    }

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

    ///
    /// Builds kafka client config
    pub fn build_client_config(&self) -> ClientConfig {
        let mut cc = ClientConfig::new();

        cc.set("bootstrap.servers", &*self.brokers)
            .set(
                "enable.auto.commit",
                format!("{}", self.config.kafka_config.enable_auto_commit),
            )
            .set(
                "auto.offset.reset",
                format!("{}", self.config.kafka_config.auto_offset_reset),
            )
            .set(
                "auto.commit.interval.ms",
                format!("{}", self.config.kafka_config.auto_commit_interval_ms),
            )
            .set(
                "enable.auto.offset.store",
                format!("{}", self.config.kafka_config.enable_auto_offset_store),
            )
            .set(
                "max.poll.interval.ms",
                format!("{}", self.config.kafka_config.max_poll_interval_ms),
            )
            .set(
                "max.partition.fetch.bytes",
                format!("{}", self.config.kafka_config.max_partition_fetch_bytes),
            )
            .set(
                "fetch.wait.max.ms",
                format!("{}", self.config.kafka_config.fetch_max_wait_ms),
            )
            .set(
                "request.timeout.ms",
                format!("{}", self.config.kafka_config.request_timeout_ms),
            )
            .set(
                "check.crcs",
                format!("{}", self.config.kafka_config.check_crcs),
            )
            .set(
                "statistics.interval.ms",
                format!("{}", self.config.kafka_config.statistics_interval_ms),
            )
            .set(
                "session.timeout.ms",
                format!("{}", self.config.kafka_config.session_timeout_ms),
            )
            .set(
                "heartbeat.interval.ms",
                format!("{}", self.config.kafka_config.heartbeat_interval_ms),
            )
            .set(
                "isolation.level",
                format!("{}", self.config.kafka_config.isolation_level),
            )
            // Consumer group ID
            .set("group.id", self.app_name.as_str());

        // Security settings
        cc.set(
            "security.protocol",
            format!("{}", self.config.kafka_config.security_protocol),
        );

        use crate::kafka::enums::SecurityProtocol::*;

        match self.config.kafka_config.security_protocol {
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
        }
    }

    fn build_sasl_context(&self, mut cc: ClientConfig) -> ClientConfig {
        self.config
            .kafka_config
            .sasl_mechanism
            .map(|e| cc.set("sasl.mechanism", format!("{}", e)));

        self.config
            .kafka_config
            .sasl_username
            .clone()
            .map(|e| cc.set("sasl.username", e));

        self.config
            .kafka_config
            .sasl_password
            .clone()
            .map(|e| cc.set("sasl.password", e));

        cc
    }

    fn build_ssl_context(&self, mut cc: ClientConfig) -> ClientConfig {
        self.config
            .kafka_config
            .ssl_certificate_location
            .clone()
            .map(|e| cc.set("ssl.certificate.location", e));

        self.config
            .kafka_config
            .ssl_ca_location
            .clone()
            .map(|e| cc.set("ssl.ca.location", e));

        self.config
            .kafka_config
            .ssl_key_location
            .clone()
            .map(|e| cc.set("ssl.key.location", e));

        self.config
            .kafka_config
            .ssl_key_password
            .clone()
            .map(|e| cc.set("ssl.key.password", e));

        self.config
            .kafka_config
            .ssl_endpoint_identification_algorithm
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
        // Recovery Service launch procedure
        if self.tables.len() > 0 {
            self.service(RecoveryService::new(
                self.app_name.clone(),
                self.state.clone(),
                self.tables.clone(),
                self.tables
                    .values()
                    .map(|e| e as Arc<dyn Service<State>>)
                    .collect::<Vec<_>>(),
            ));
        }

        // Web Service launch procedure
        if self.routes.len() > 0 {
            self.service(Web::new(
                self.app_name.clone(),
                self.state.clone(),
                self.routes.values().into_iter().collect(),
                vec![],
            ));
        }

        Ok(())
    }

    ///
    /// Dynamic runner for [Agent].
    /// Allows user to run a dynamic agent while application is running.
    /// This agent is not bookkept, instead user can manage the lifecycle
    /// of the application with [nuclei::Task] handler.
    pub fn run_agent(agent: Arc<dyn Agent<State>>) -> AsyncTask<()> {
        info!("Starting dynamic Agent");

        nuclei::spawn(async move {
            match agent.start().await {
                Ok(dep) => dep.await,
                _ => panic!(
                    "Error occurred on start of dynamic Agent with label: {}.",
                    agent.label().await
                ),
            }

            agent.after_start().await;
        })
    }



    pub fn run(self) {
        // Load all background workers
        self.background_workers();

        let agents: Vec<AsyncTask<()>> = self
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

        let agent_handles = join_all(agents);

        let mut flows: Vec<AsyncTask<()>> = self
            .flows
            .iter()
            .map(|(fid, flow)| {
                info!("Starting Flow with ID: {}", fid);
                // TODO: Recovery should be here.
                nuclei::spawn(async move {
                    match flow.start().await {
                        Ok(dep) => dep.await,
                        _ => panic!("Error occurred on start of Flow with ID: {}.", fid),
                    }

                    flow.after_start().await;
                })
            })
            .collect();

        let flow_handles = join_all(flows);

        let table_agents: Vec<AsyncTask<()>> = self
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

        let table_agent_handles = join_all(table_agents);

        let services: Vec<AsyncTask<()>> = self
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

        let service_handles = join_all(services);

        let tasks: Vec<AsyncTask<()>> = self
            .tasks
            .iter()
            .map(|(tid, task)| {
                info!("Starting Task with ID: {}", tid);
                // TODO: Recovery should be here.
                nuclei::spawn(async move {
                    match task.start().await {
                        Ok(dep) => dep.await,
                        _ => panic!("Error occurred on start of Task with ID: {}.", tid),
                    }
                })
            })
            .collect();

        let task_handles = join_all(tasks);

        let timers: Vec<AsyncTask<()>> = self
            .timers
            .iter()
            .map(|(tid, timer)| {
                info!("Starting Timer with ID: {}", tid);
                // TODO: Recovery should be here.
                nuclei::spawn(async move {
                    match timer.start().await {
                        Ok(dep) => dep.await,
                        _ => panic!("Error occurred on start of Task with ID: {}.", tid),
                    }
                })
            })
            .collect();

        let timer_handles = join_all(timers);

        let agent_poller = nuclei::spawn_blocking(move || nuclei::block_on(agent_handles));
        let flow_poller = nuclei::spawn_blocking(move || nuclei::block_on(flow_handles));
        let table_agent_poller =
            nuclei::spawn_blocking(move || nuclei::block_on(table_agent_handles));
        let service_poller = nuclei::spawn_blocking(move || nuclei::block_on(service_handles));
        let task_poller = nuclei::spawn_blocking(move || nuclei::block_on(task_handles));
        let timer_poller = nuclei::spawn_blocking(move || nuclei::block_on(timer_handles));

        #[cfg(feature = "onthefly")]
        {
            nuclei::spawn_blocking(move || nuclei::block_on(
                join_all(vec![
                    agent_poller,
                    table_agent_poller,
                    service_poller,
                    task_poller,
                    timer_poller,
                    flow_poller
                ])
            ));

            loop {
                nuclei::block_on(async move {
                    Delay::new(Duration::from_secs(5)).await;
                });

                let onthefly = async {
                    let mut agents: Vec<AsyncTask<()>> = Vec::new();

                    for (aid, agent) in self.agents.iter() {
                        if !agent.started().await {
                            info!("Starting Agent with ID: {}", aid);
                            // TODO: Recovery should be here.
                            let task = nuclei::spawn(async move {
                                match agent.start().await {
                                    Ok(dep) => dep.await,
                                    _ => panic!("Error occurred on start of Agent with ID: {}.", aid),
                                }

                                agent.after_start().await;
                            });
                            agents.push(task);
                        }
                    }

                    let agent_handles = join_all(agents);

                    let mut table_agents: Vec<AsyncTask<()>> = Vec::new();

                    for (aid, table_agent) in self.table_agents.iter() {
                        if !table_agent.started().await {
                            info!("Starting Table Agent with ID: {}", aid);
                            // TODO: Recovery should be here.
                            let task = nuclei::spawn(async move {
                                match table_agent.start().await {
                                    Ok(dep) => dep.await,
                                    _ => panic!("Error occurred on start of Table Agent with ID: {}.", aid),
                                }

                                table_agent.after_start().await;
                            });
                            table_agents.push(task);
                        }
                    }

                    let table_agent_handles = join_all(table_agents);

                    let mut services: Vec<AsyncTask<()>> = Vec::new();

                    for (sid, service) in self.services.iter() {
                        if !service.started().await {
                            info!("Starting Service with ID: {}", sid);
                            // TODO: Recovery should be here.
                            let task = nuclei::spawn(async move {
                                match service.start().await {
                                    Ok(dep) => dep.await,
                                    _ => panic!("Error occurred on start of Service with ID: {}.", sid),
                                }

                                service.after_start().await;
                            });
                            services.push(task);
                        }
                    }

                    let service_handles = join_all(services);

                    let agent_poller = nuclei::spawn_blocking(move || nuclei::block_on(agent_handles));
                    let table_agent_poller =
                        nuclei::spawn_blocking(move || nuclei::block_on(table_agent_handles));
                    let service_poller = nuclei::spawn_blocking(move || nuclei::block_on(service_handles));

                    nuclei::spawn_blocking(move || nuclei::block_on(
                        join_all(vec![
                            agent_poller,
                            table_agent_poller,
                            service_poller
                        ])
                    ));
                };


                nuclei::block_on(async move {
                    Delay::new(Duration::from_secs(10)).await;
                });
            }
        }

        #[cfg(not(feature = "onthefly"))]
        {
            nuclei::block_on(async move {
                join_all(vec![
                    agent_poller,
                    table_agent_poller,
                    service_poller,
                    task_poller,
                    timer_poller,
                    flow_poller
                ])
                    .await
            });
        }
    }
}
