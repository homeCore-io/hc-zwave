//! `hc-zwave` — HomeCore plugin for Z-Wave via the zwave-js-server WebSocket API.
//!
//! Connects directly to a running `zwave-js-server` (bundled inside ZwaveJS UI or
//! standalone), receives live Z-Wave events, and publishes canonical device state
//! to the HomeCore MQTT broker.  Outbound commands (`homecore/devices/zwave_+/cmd`)
//! are translated to `node.set_value` WebSocket calls.
//!
//! ## Usage
//!
//! ```sh
//! hc-zwave [config/config.toml]
//! ```

mod bridge;
mod config;
mod logging;
mod translator;
mod types;

use bridge::Bridge;
use config::Config;
use std::path::{Path, PathBuf};
use tracing::error;

#[tokio::main]
async fn main() {
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config/config.toml".to_string());

    let _log_guard = init_logging(&config_path);

    let cfg = match Config::load(&config_path) {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, path = %config_path, "Failed to load config");
            std::process::exit(1);
        }
    };

    tracing::info!(
        config      = %config_path,
        plugin_id   = %cfg.homecore.plugin_id,
        broker_host = %cfg.homecore.broker_host,
        broker_port = cfg.homecore.broker_port,
        server_url  = %cfg.server.url,
        "hc-zwave starting",
    );

    if let Err(e) = (Bridge {
        config: cfg,
        published_ids_cache_path: published_ids_cache_path(&config_path),
    })
    .run()
    .await
    {
        error!(error = %e, "Bridge exited with error");
        std::process::exit(1);
    }
}

// ---------------------------------------------------------------------------
// Logging: stderr (RUST_LOG filtered) + rotating compressed file in logs/
// ---------------------------------------------------------------------------

fn init_logging(config_path: &str) -> tracing_appender::non_blocking::WorkerGuard {
    #[derive(serde::Deserialize, Default)]
    struct Bootstrap {
        #[serde(default)]
        logging: logging::LoggingConfig,
    }
    let bootstrap: Bootstrap = std::fs::read_to_string(config_path)
        .ok()
        .and_then(|s| toml::from_str(&s).ok())
        .unwrap_or_default();
    logging::init_logging(config_path, "hc-zwave", "hc_zwave=info", &bootstrap.logging)
}

fn published_ids_cache_path(config_path: &str) -> PathBuf {
    Path::new(config_path)
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(".published-device-ids.json")
}
