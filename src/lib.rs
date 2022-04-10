//!
//! Callysto is a Rust framework for stream processing,
//! which benefits from Rust's concurrency model and
//! optimized performance.
//!
//! Callysto made available with Kafka Streams mentality in Rust.
//! It is for building high-performance distributed system and near real-time data
//! pipelines to process large amounts of events.
//!
//! Callysto behaves like both stream processing and event processing,
//! sharing similarity with tools such as Faust, Kafka Streams, Apache Spark/Storm/Samza/Flink.
//!
//! It is built on service oriented model to build microservices.

#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused_must_use)]
#![allow(unused)]
#![allow(incomplete_features)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/vertexclique/callysto/master/art/callysto_logo.png"
)]
// Force missing implementations
// #![warn(missing_docs)]
// #![warn(missing_debug_implementations)]
// Doc generation experimental features
#![cfg_attr(feature = "docs", feature(doc_cfg))]

/// Application builder methods
///
/// # Example
/// ```rust,no_run
/// use callysto::prelude::*;
///
/// let mut app = Callysto::new();
/// app.with_name("example-app");
/// app.run();
/// ```
pub mod app;
/// Application configuration
pub mod config;
/// Errors of Callysto
pub mod errors;
/// Methods to interact with Kafka
pub mod kafka;
/// Metric implementations to instrument Callysto application
pub mod sensors;
/// Callysto's building blocks. All modules are under types.
pub mod types;

mod runtime;
mod stores;

/// Reexport of Futures
pub use futures;
/// Reexport of http_types
pub use http_types;
/// Reexport of rdkafka
pub use rdkafka;

/// Callysto prelude
pub mod prelude {
    pub use super::app::*;
    pub use super::config::*;
    pub use super::errors::*;
    pub use super::http_types::{
        Request as CWebRequest, Response as CWebResponse, Result as CWebResult,
    };
    pub use super::kafka::prelude::*;
    pub use super::rdkafka::*;
    pub use super::types::prelude::*;
    pub use super::types::table::*;
}
