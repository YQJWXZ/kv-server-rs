use serde::{Deserialize, Serialize};

use crate::KvError;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ServerConfig {
    pub general: GeneralConfig,
    pub storage: StorageConfig,
    pub tls: ServerTlsConfig,
    pub log: LogConfig,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClientConfig {
    pub general: GeneralConfig,
    pub tls: ClientTlsConfig,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GeneralConfig {
    pub addr: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogConfig {
    pub path: String,
    pub rotation: RotationConfig,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum RotationConfig {
    Hourly,
    Daily,
    Never,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "args")]
pub enum StorageConfig {
    MemTable,
    SledDb(String),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ServerTlsConfig {
    pub cert: String,
    pub key: String,
    pub ca: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClientTlsConfig {
    pub domain: String,
    pub identity: Option<(String, String)>,
    pub ca: Option<String>,
}

impl ServerConfig {
    pub async fn load(path: &str) -> Result<Self, KvError> {
        let config = tokio::fs::read_to_string(path).await?;
        let config: Self = toml::from_str(&config)?;
        Ok(config)
    }
}

impl ClientConfig {
    pub async fn load(path: &str) -> Result<Self, KvError> {
        let config = tokio::fs::read_to_string(path).await?;
        let config: Self = toml::from_str(&config)?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_config_should_work() {
        let result: Result<ServerConfig, toml::de::Error> =
            toml::from_str(include_str!("../fixtures/server.conf"));
        assert!(result.is_ok());
    }

    #[test]
    fn client_config_should_work() {
        let result: Result<ClientConfig, toml::de::Error> =
            toml::from_str(include_str!("../fixtures/client.conf"));
        assert!(result.is_ok());
    }
}
