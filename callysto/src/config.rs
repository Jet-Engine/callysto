use crate::kafka::enums::*;

#[derive(Clone, Default)]
pub struct Config {
    ///
    /// Kafka Config for the service
    pub kafka_config: KafkaConfig,
}

impl Config {
    fn validate(&self) {
        todo!("Validate")
    }
}

#[derive(Clone)]
pub struct KafkaConfig {
    ///
    /// Processing guarantee that should be used.
    pub processing_guarantee: ProcessingGuarantee,

    // --------- Kafka Related ---------
    ///
    /// How long to wait for a node to finish rebalancing before the broker will consider it dysfunctional and remove it from the cluster.
    /// Increase this if you experience the cluster being in a state of constantly rebalancing,
    /// but make sure you also increase the broker_heartbeat_interval at the same time.
    ///
    /// This configuration is in milliseconds format.
    pub session_timeout_ms: usize,
    ///
    /// Auto commit (Mind that when enabled guarantees are not going to applied internally.
    pub enable_auto_commit: bool,

    ///
    /// When the group is first created, before any messages have been consumed, the position is set
    /// according to a configurable offset reset policy (auto.offset.reset).
    /// Typically, consumption starts either at the earliest offset or the latest offset.
    pub auto_offset_reset: OffsetReset,

    ///
    /// Auto commit interval ms
    pub auto_commit_interval_ms: usize,

    ///
    /// Select storing offsets explicitly inside the library for committing via auto commit.
    /// Use `consumer.store_offset` to explicitly store the offset.
    pub enable_auto_offset_store: bool,

    ///
    /// The maximum allowed time (in milliseconds) between calls to consume messages
    /// If the consumer crashes or is unable to send heartbeats for a duration of `session.timeout.ms` (in Callysto it is `session_timeout_ms`),
    /// then the consumer will be considered dead and its partitions will be reassigned.
    pub max_poll_interval_ms: usize,

    ///
    /// The maximum amount of time the client will wait for the response of a request.
    /// If the response is not received before the timeout elapses the client will resend the
    /// request if necessary or fail the request if retries are exhausted.
    pub request_timeout_ms: usize,

    ///
    /// The expected time in milliseconds between heartbeats to the consumer coordinator when
    /// using Kafka’s group management feature. Heartbeats are used to ensure that the consumer’s
    /// session stays active and to facilitate rebalancing when new consumers join or
    /// leave the group.
    ///
    /// The value must be set lower than `session_timeout_ms`, but typically should be set no
    /// higher than 1/3 of that value. It can be adjusted even lower to control the expected
    /// time for normal rebalances.
    pub heartbeat_interval_ms: usize,

    ///
    /// Controls how to read messages written transactionally:
    ///
    /// * `read_committed` - only return transactional messages which have been committed.
    /// * `read_uncommitted` - return all messages, even transactional messages which have been aborted.
    pub isolation_level: IsolationLevel,

    ///
    /// Alias for `fetch.message.max.bytes`: Initial maximum number of bytes per topic+partition
    /// to request when fetching messages from the broker. If the client encounters a message larger
    /// than this value it will gradually try to increase it until the entire message can be fetched.
    pub max_partition_fetch_bytes: usize,

    ///
    /// The maximum amount of time the server will block before answering the fetch request if there isn't
    /// sufficient data to immediately satisfy the requirement given by fetch.min.bytes.
    pub fetch_max_wait_ms: usize,

    ///
    /// Automatically check the CRC32 of the records consumed. This ensures no on-the-wire or on-disk corruption
    /// to the messages occurred. This check adds some overhead, so it may be disabled in cases seeking
    /// extreme performance.
    pub check_crcs: bool,

    ///
    /// Statistics emit interval
    pub statistics_interval_ms: usize,

    ///
    /// Protocol used to communicate with brokers.
    pub security_protocol: SecurityProtocol,

    /// SSL Context variable:
    /// Certificate Authority Location
    pub ssl_ca_location: Option<String>,
    /// SSL Context variable:
    /// Certificate Location
    pub ssl_certificate_location: Option<String>,
    /// SSL Context variable:
    /// SSL Key Location
    pub ssl_key_location: Option<String>,
    /// SSL Context variable:
    /// SSL Password Location
    pub ssl_key_password: Option<String>,

    /// Endpoint identification algorithm to validate broker hostname using broker certificate.
    /// * https - Server (broker) hostname verification as specified in RFC2818.
    /// * none - No endpoint verification. OpenSSL >= 1.0.2 required.
    pub ssl_endpoint_identification_algorithm: Option<EndpointIdentificationAlgorithm>,

    /// SASL Context
    /// SASL Mechanism name
    /// Only one mechanism is allowed to be configured.
    pub sasl_mechanism: Option<SaslMechanism>,

    /// SASL Username
    pub sasl_username: Option<String>,
    /// SASL Password
    pub sasl_password: Option<String>,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            processing_guarantee: ProcessingGuarantee::AtLeastOnce,
            session_timeout_ms: 60_000,
            enable_auto_commit: false,
            auto_offset_reset: OffsetReset::Latest,
            auto_commit_interval_ms: 5_000,
            enable_auto_offset_store: false,
            max_poll_interval_ms: 70_000, // 16.67 mins
            request_timeout_ms: 90_000,
            heartbeat_interval_ms: 3_000,
            isolation_level: IsolationLevel::ReadUncommitted,
            max_partition_fetch_bytes: 1024 * 1024,
            fetch_max_wait_ms: 1500,
            check_crcs: true,
            statistics_interval_ms: 200,
            security_protocol: SecurityProtocol::Plaintext,
            ssl_ca_location: None,
            ssl_certificate_location: None,
            ssl_key_location: None,
            ssl_key_password: None,
            ssl_endpoint_identification_algorithm: None,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
        }
    }
}
