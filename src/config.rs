//! Plugin configuration — loaded from `config/config.toml`.
//!
//! See `config/config.toml.example` for a fully annotated reference.

use anyhow::Result;
use serde::Deserialize;

// ---------------------------------------------------------------------------
// Top-level config
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub homecore: HomecoreConfig,
    pub server: ServerConfig,
    #[serde(default)]
    pub logging: crate::logging::LoggingConfig,
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("Cannot read config {path}: {e}"))?;
        toml::from_str(&text)
            .map_err(|e| anyhow::anyhow!("Config parse error in {path}: {e}"))
    }
}

// ---------------------------------------------------------------------------
// [homecore] — MQTT broker connection and plugin identity
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct HomecoreConfig {
    #[serde(default = "default_broker_host")]
    pub broker_host: String,
    #[serde(default = "default_broker_port")]
    pub broker_port: u16,
    #[serde(default = "default_plugin_id")]
    pub plugin_id: String,
    #[serde(default)]
    pub password: String,
}

fn default_broker_host() -> String { "127.0.0.1".into() }
fn default_broker_port() -> u16    { 1883 }
fn default_plugin_id()   -> String { "plugin.zwave".into() }

// ---------------------------------------------------------------------------
// [server] — zwave-js-server WebSocket endpoint
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    /// WebSocket URL of the zwave-js-server, e.g. `"ws://localhost:3000"`.
    pub url: String,
    /// Schema version to negotiate. Clamped to the server's advertised min/max.
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
}

fn default_schema_version() -> u32 { 32 }
