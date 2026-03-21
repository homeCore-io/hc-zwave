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
mod translator;
mod types;

use bridge::Bridge;
use config::Config;
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

    if let Err(e) = (Bridge { config: cfg }).run().await {
        error!(error = %e, "Bridge exited with error");
        std::process::exit(1);
    }
}

// ---------------------------------------------------------------------------
// Logging: stderr (RUST_LOG filtered) + rolling daily file in logs/
// ---------------------------------------------------------------------------

fn init_logging(config_path: &str) -> tracing_appender::non_blocking::WorkerGuard {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

    let log_dir = std::path::Path::new(config_path)
        .parent()                          // config/
        .and_then(|p| p.parent())          // plugin root
        .map(|p| p.join("logs"))
        .unwrap_or_else(|| std::path::PathBuf::from("logs"));
    std::fs::create_dir_all(&log_dir).ok();

    let file_appender = tracing_appender::rolling::daily(&log_dir, "hc-zwave.log");
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    let stderr_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "hc_zwave=info".parse().unwrap());

    let stderr_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_filter(stderr_filter);

    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking)
        .with_ansi(false)
        .with_filter(EnvFilter::new("debug"));

    tracing_subscriber::registry()
        .with(stderr_layer)
        .with(file_layer)
        .init();

    guard
}
