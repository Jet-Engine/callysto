#[cfg(feature = "sensor_datadog")]
#[cfg_attr(feature = "docs", doc(cfg(sensor_datadog)))]
/// DataDog sensor for Callysto
pub mod datadog;

/// Sensor interface for Callysto
pub mod sensor;
