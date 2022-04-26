#[cfg(feature = "sink_elastic")]
#[cfg_attr(feature = "docs", doc(cfg(sink_elastic)))]
/// ElasticSearch Sink for CStream
pub mod elasticsearch;

#[cfg(feature = "sink_postgres")]
#[cfg_attr(feature = "docs", doc(cfg(sink_postgres)))]
/// Postgres Sink for CStream
pub mod postgres;
