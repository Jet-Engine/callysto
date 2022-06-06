use crate::config::DatadogConfig;
use crate::sensors::sensor::CSensor;
use opentelemetry::sdk::trace;
use opentelemetry::sdk::trace::{Sampler, Tracer};
use opentelemetry_datadog::{new_pipeline, ApiVersion};
use tracing::{warn, Subscriber};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{EnvFilter, Layer, Registry};

pub struct CDataDogSensor {
    app_name: String,
    config: DatadogConfig,
}

impl CDataDogSensor {
    pub fn new(app_name: String, config: DatadogConfig) -> Self {
        Self { app_name, config }
    }

    pub fn build_layer<S>(&self) -> Box<dyn Layer<S>>
    where
        S: Subscriber + for<'span> LookupSpan<'span>,
    {
        let api_version = match self.config.api_version {
            3 => ApiVersion::Version03,
            5 => ApiVersion::Version05,
            _ => {
                warn!("Defaulting to DataDog API Version 5.");
                ApiVersion::Version05
            }
        };
        let dpb = new_pipeline()
            .with_service_name(self.app_name.as_str())
            .with_version(api_version)
            .with_agent_endpoint(self.config.agent_endpoint.as_str())
            .with_trace_config(trace::config().with_sampler(Sampler::AlwaysOn));

        #[cfg(feature = "asyncexec")]
        let ins = dpb.install_batch(opentelemetry::runtime::AsyncStd);
        #[cfg(feature = "tokio")]
        let ins = dpb.install_batch(opentelemetry::runtime::Tokio);

        let tracer: Tracer = ins.unwrap();
        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
        Box::new(telemetry)
    }
}

impl CSensor for CDataDogSensor {}
