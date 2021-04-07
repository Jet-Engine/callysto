use crate::enums::{OffsetReset, ProcessingGuarantee};

pub struct Config {
    ///
    /// Daemonize the service
    daemonize: bool,

    ///
    /// Processing guarantee that should be used.
    processing_guarantee: ProcessingGuarantee,

    // --------- Kafka Related ---------
    ///
    /// How long to wait for a node to finish rebalancing before the broker will consider it dysfunctional and remove it from the cluster.
    /// Increase this if you experience the cluster being in a state of constantly rebalancing,
    /// but make sure you also increase the broker_heartbeat_interval at the same time.
    ///
    /// This configuration is in milliseconds format.
    session_timeout_ms: usize,
    ///
    /// Auto commit (Mind that when enabled guarantees are not going to applied internally.
    enable_auto_commit: bool,

    ///
    /// When the group is first created, before any messages have been consumed, the position is set
    /// according to a configurable offset reset policy (auto.offset.reset).
    /// Typically, consumption starts either at the earliest offset or the latest offset.
    auto_offset_reset: OffsetReset,

    ///
    /// Auto commit interval ms
    auto_commit_interval_ms: usize,

    ///
    /// Select storing offsets explicitly inside the library for committing via auto commit.
    /// Use `consumer.store_offset` to explicitly store the offset.
    enable_auto_offset_store: bool,

    ///
    /// The maximum allowed time (in milliseconds) between calls to consume messages
    /// If the consumer crashes or is unable to send heartbeats for a duration of `session.timeout.ms` (in Callysto it is [session_timeout_ms]),
    /// then the consumer will be considered dead and its partitions will be reassigned.
    max_poll_interval_ms: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            daemonize: false,
            processing_guarantee: ProcessingGuarantee::AtLeastOnce,
            session_timeout_ms: 6_000,
            enable_auto_commit: false,
            auto_offset_reset: OffsetReset::Latest,
            auto_commit_interval_ms: 5_000,
            enable_auto_offset_store: false,
            max_poll_interval_ms: 1_000_000 // 16.67 mins
        }
    }
}