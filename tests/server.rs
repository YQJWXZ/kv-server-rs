use std::time::Duration;

use anyhow::Result;
use kv_server_rs::{
    start_client_with_config, start_server_with_config, ClientConfig, CommandRequest, ServerConfig,
    StorageConfig,
};
use tokio::time;

#[tokio::test]
async fn yamux_server_client_full_tests() -> Result<()> {
    let addr = "127.0.0.1:8090";
    let mut config: ServerConfig = toml::from_str(include_str!("../fixtures/server.conf"))?;
    config.general.addr = addr.into();
    config.storage = StorageConfig::MemTable;

    tokio::spawn(async move {
        start_server_with_config(&config).await.unwrap();
    });

    time::sleep(Duration::from_millis(10)).await;

    let mut config: ClientConfig = toml::from_str(include_str!("../fixtures/client.conf"))?;
    config.general.addr = addr.into();

    let mut ctrl = start_client_with_config(&config).await.unwrap();
    let mut stream = ctrl.open_stream().await?;

    let cmd = CommandRequest::hset("t1", "hello", "world".to_string().into());
    stream.execute_unary(&cmd).await?;

    let cmd = CommandRequest::hget("t1", "hello");
    let resp = stream.execute_unary(&cmd).await?;

    assert_eq!(resp.status, 200);
    assert_eq!(resp.values, &["world".into()]);

    Ok(())
}
