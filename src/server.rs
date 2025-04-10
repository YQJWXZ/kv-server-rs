use std::env;

use anyhow::Result;
use kv_server_rs::{start_server_with_config, RotationConfig, ServerConfig};
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::{Protocol, WithExportConfig};
use tracing_subscriber::{
    fmt::{self, format},
    layer::SubscriberExt,
    prelude::*,
    EnvFilter,
};

#[tokio::main]
async fn main() -> Result<()> {
    run_server().await
}

fn setup_tracing(config: &ServerConfig) -> Result<()> {
    let otlp_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .build()?;

    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(otlp_exporter)
        .build();

    let tracer = provider.tracer("kv-server");

    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    let log = &config.log;
    let file_appender = match log.rotation {
        RotationConfig::Hourly => tracing_appender::rolling::hourly(&log.path, "server.log"),
        RotationConfig::Daily => tracing_appender::rolling::daily(&log.path, "server.log"),
        RotationConfig::Never => tracing_appender::rolling::never(&log.path, "server.log"),
    };

    let (non_blocking, _guard1) = tracing_appender::non_blocking(file_appender);
    let fmt_layer = fmt::layer()
        .event_format(format().compact())
        .with_writer(non_blocking);

    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(fmt_layer)
        .with(opentelemetry)
        .init();

    Ok(())
}

async fn run_server() -> Result<()> {
    let config = match env::var("KV_SERVER_CONFIG") {
        Ok(path) => ServerConfig::load(&path).await?,
        Err(_) => {
            let config_str = include_str!("../fixtures/server.conf").to_string();
            toml::from_str(&config_str)?
        }
    };

    setup_tracing(&config)?;

    start_server_with_config(&config).await?;

    Ok(())
}
