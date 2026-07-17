//! Plugin configuration — loaded from `config/config.toml`.
//!
//! See `config/config.toml.example` for a fully annotated reference.

use anyhow::Result;
use serde::Deserialize;

/// Operator-config JSON Schema, published on the capability manifest so the
/// hc-web editor renders a typed form. `None` without the `schema` feature.
#[cfg(feature = "schema")]
pub fn config_schema() -> Option<serde_json::Value> {
    serde_json::to_value(schemars::schema_for!(Config)).ok()
}

#[cfg(not(feature = "schema"))]
pub fn config_schema() -> Option<serde_json::Value> {
    None
}

// ---------------------------------------------------------------------------
// Top-level config
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Config {
    pub homecore: HomecoreConfig,
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub logging: crate::logging::LoggingConfig,
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("Cannot read config {path}: {e}"))?;
        toml::from_str(&text).map_err(|e| anyhow::anyhow!("Config parse error in {path}: {e}"))
    }
}

// ---------------------------------------------------------------------------
// [homecore] — MQTT broker connection and plugin identity
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
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

fn default_broker_host() -> String {
    "127.0.0.1".into()
}
fn default_broker_port() -> u16 {
    1883
}
fn default_plugin_id() -> String {
    "plugin.zwave".into()
}

// ---------------------------------------------------------------------------
// [server] — zwave-js-server WebSocket endpoint
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ServerConfig {
    /// WebSocket URL of the zwave-js-server, e.g. `"ws://localhost:3000"`.
    #[serde(default = "default_server_url")]
    pub url: String,
    /// Schema version to negotiate. Clamped to the server's advertised min/max.
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            url: default_server_url(),
            schema_version: default_schema_version(),
        }
    }
}

fn default_server_url() -> String {
    "ws://localhost:3000".into()
}
fn default_schema_version() -> u32 {
    32
}
