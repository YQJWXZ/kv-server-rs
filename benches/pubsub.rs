use std::time::Duration;
use tokio::time;

use anyhow::Result;
use criterion::{criterion_group, criterion_main, Criterion};
use futures::StreamExt;
use kv_server_rs::{
    start_client_with_config, start_server_with_config, ClientConfig, CommandRequest, ServerConfig,
    StorageConfig, YamuxCtrl,
};
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::{Protocol, WithExportConfig};
use rand::seq::IndexedRandom;
use tokio::{net::TcpStream, runtime::Builder};
use tokio_rustls::client::TlsStream;
use tracing::{info, span};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
async fn start_server() -> Result<()> {
    let addr = "127.0.0.1:8086";
    let mut config: ServerConfig = toml::from_str(include_str!("../fixtures/server.conf"))?;
    config.general.addr = addr.into();
    config.storage = StorageConfig::MemTable;

    tokio::spawn(async move {
        start_server_with_config(&config).await.unwrap();
    });

    Ok(())
}

async fn connect() -> Result<YamuxCtrl<TlsStream<TcpStream>>> {
    let addr = "127.0.0.1:8086";
    let mut config: ClientConfig = toml::from_str(include_str!("../fixtures/client.conf"))?;
    config.general.addr = addr.into();

    Ok(start_client_with_config(&config).await.unwrap())
}

async fn start_subscribers(topic: &'static str) -> Result<()> {
    let mut ctrl = connect().await?;
    let stream = ctrl.open_stream().await?;
    info!("C(subscriber): stream opened");

    let cmd = CommandRequest::subscribe(topic.to_string());
    tokio::spawn(async move {
        let mut stream = stream.execute_streaming(&cmd).await.unwrap();
        while let Some(Ok(data)) = stream.next().await {
            drop(data);
        }
    });

    Ok(())
}

async fn start_publishers(topic: &'static str, values: &'static [&'static str]) -> Result<()> {
    let mut rng = rand::rng();
    let v = values.choose(&mut rng).unwrap();

    let mut ctrl = connect().await.unwrap();
    let mut stream = ctrl.open_stream().await.unwrap();
    info!("C(publisher): stream opened");

    let cmd = CommandRequest::publish(topic.to_string(), vec![(*v).into()]);
    stream.execute_unary(&cmd).await.unwrap();

    Ok(())
}

fn init_tracer(
) -> Result<opentelemetry_sdk::trace::Tracer, Box<dyn std::error::Error + Send + Sync>> {
    let otlp_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .build()
        .unwrap();

    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(otlp_exporter)
        .build();

    let tracer = provider.tracer("kv-server");

    Ok(tracer)
}

fn pubsub(c: &mut Criterion) {
    let tracer = init_tracer().unwrap();
    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(opentelemetry)
        .init();

    let root = span!(tracing::Level::INFO, "app_start", work_units = 2);
    let _enter = root.enter();

    let runtime = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .thread_name("pubsub")
        .build()
        .unwrap();

    let base_str = include_str!("../fixtures/server.conf");

    let values: &'static [&'static str] = Box::leak(
        vec![
            &base_str[..64],
            &base_str[..128],
            &base_str[..256],
            &base_str[..512],
        ]
        .into_boxed_slice(),
    );
    let topic = "lobby";

    runtime.block_on(async {
        eprint!("preparing server and subscribers...");
        start_server().await.unwrap();
        time::sleep(Duration::from_millis(50)).await;
        for _ in 0..1000 {
            start_subscribers(topic).await.unwrap();
            eprint!(".");
        }

        eprintln!("Done");
    });

    c.bench_function("publishing", |b| {
        b.to_async(&runtime)
            .iter(|| async { start_publishers(topic, values).await })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = pubsub
}
criterion_main!(benches);
