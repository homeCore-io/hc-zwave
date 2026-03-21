//! Bridge between zwave-js-server WebSocket and HomeCore MQTT.
//!
//! # Flow
//!
//! 1. Connect MQTT → subscribe `homecore/devices/zwave_+/cmd`
//! 2. Connect WebSocket → handshake (set_api_schema → start_listening)
//! 3. Publish full state for all nodes from the start_listening result
//! 4. Event loop:
//!    - WS `value updated`       → translate → `homecore/devices/zwave_N/state/partial`
//!    - WS `node status changed` → `homecore/devices/zwave_N/availability`
//!    - WS `node name updated`   → partial state `{"name": "..."}`
//!    - WS `node ready`          → republish full node state
//!    - MQTT cmd                 → `node.set_value` WebSocket command
//! 5. Reconnect on WS disconnect with exponential back-off

use crate::config::Config;
use crate::translator::{property_key_str, Translator};
use crate::types::{NodeState, NodeValue, ResultMsg, ServerMsg, ValueUpdatedArgs};
use anyhow::{bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// MQTT helpers
// ---------------------------------------------------------------------------

async fn connect_mqtt(cfg: &Config) -> Result<(AsyncClient, EventLoop)> {
    let mut opts = MqttOptions::new(&cfg.homecore.plugin_id, &cfg.homecore.broker_host, cfg.homecore.broker_port);
    opts.set_keep_alive(Duration::from_secs(30));
    opts.set_clean_session(true);
    if !cfg.homecore.password.is_empty() {
        opts.set_credentials(&cfg.homecore.plugin_id, &cfg.homecore.password);
    }
    let (client, eventloop) = AsyncClient::new(opts, 128);
    Ok((client, eventloop))
}

async fn publish_state(client: &AsyncClient, device_id: &str, state: &Value) -> Result<()> {
    let topic = format!("homecore/devices/{device_id}/state");
    client
        .publish(&topic, QoS::AtLeastOnce, true, serde_json::to_vec(state)?)
        .await
        .context("publish state")?;
    Ok(())
}

async fn publish_partial(client: &AsyncClient, device_id: &str, patch: &Value) -> Result<()> {
    let topic = format!("homecore/devices/{device_id}/state/partial");
    client
        .publish(&topic, QoS::AtLeastOnce, false, serde_json::to_vec(patch)?)
        .await
        .context("publish partial")?;
    Ok(())
}

async fn publish_availability(client: &AsyncClient, device_id: &str, available: bool) -> Result<()> {
    let topic = format!("homecore/devices/{device_id}/availability");
    let payload = if available { "online" } else { "offline" };
    client
        .publish(&topic, QoS::AtLeastOnce, true, payload.as_bytes())
        .await
        .context("publish availability")?;
    Ok(())
}

async fn register_node(
    client: &AsyncClient,
    plugin_id: &str,
    device_id: &str,
    name: &str,
) -> Result<()> {
    let topic = format!("homecore/plugins/{plugin_id}/register");
    let payload = serde_json::json!({
        "device_id": device_id,
        "plugin_id": plugin_id,
        "name":      name,
        "device_type": "zwave",
    });
    client
        .publish(&topic, QoS::AtLeastOnce, false, serde_json::to_vec(&payload)?)
        .await
        .context("register_node failed")?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Node state → MQTT
// ---------------------------------------------------------------------------

fn node_device_id(node_id: u32) -> String {
    format!("zwave_{node_id}")
}

/// Build a full state map from a NodeState, translating each value via the alias table.
fn build_state(node: &NodeState, translator: &Translator) -> Value {
    let mut map = serde_json::Map::new();

    // Include name/location if present
    if let Some(name) = &node.name {
        if !name.is_empty() {
            map.insert("name".into(), Value::String(name.clone()));
        }
    }
    if let Some(loc) = &node.location {
        if !loc.is_empty() {
            map.insert("location".into(), Value::String(loc.clone()));
        }
    }

    for v in &node.values {
        translate_node_value(v, translator, &mut map);
    }

    Value::Object(map)
}

fn translate_node_value(
    v: &NodeValue,
    translator: &Translator,
    map: &mut serde_json::Map<String, Value>,
) {
    let raw = match &v.value {
        Some(val) if !val.is_null() => val,
        _ => return,
    };
    let pk_str = v.property_key.as_ref().and_then(property_key_str);
    let pk = pk_str.as_deref();

    if let Some((attr, val)) = translator.translate(v.command_class, v.endpoint, &v.property, pk, raw) {
        map.insert(attr, val);
    }
}

async fn publish_node(client: &AsyncClient, plugin_id: &str, node: &NodeState, translator: &Translator) -> Result<()> {
    let device_id = node_device_id(node.node_id);
    let display_name = node.name.as_deref().filter(|n| !n.is_empty()).unwrap_or(&device_id).to_string();
    register_node(client, plugin_id, &device_id, &display_name).await?;

    // Diagnostic: log all CC 98 (Door Lock) value IDs so we can see exactly what the
    // device reports and verify the write target (targetMode) exists on this node.
    let cc98_values: Vec<String> = node
        .values
        .iter()
        .filter(|v| v.command_class == 98)
        .map(|v| {
            let pk = v.property_key.as_ref().map(|pk| format!("[{pk}]")).unwrap_or_default();
            let val = v.value.as_ref().map(|val| format!("={val}")).unwrap_or_default();
            format!("{}{}{}", v.property, pk, val)
        })
        .collect();
    if !cc98_values.is_empty() {
        info!(node_id = node.node_id, values = ?cc98_values, "Door Lock CC 98 value IDs on this node");
    }

    let state = build_state(node, translator);
    publish_state(client, &device_id, &state).await?;
    publish_availability(client, &device_id, node.is_available()).await?;
    debug!(node_id = node.node_id, device_id, "Published full node state");
    Ok(())
}

// ---------------------------------------------------------------------------
// WebSocket handshake
// ---------------------------------------------------------------------------

type WsSink = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    Message,
>;
type WsStream = futures_util::stream::SplitStream<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
>;

async fn ws_send(tx: &mut WsSink, msg: &Value) -> Result<()> {
    let text = serde_json::to_string(msg)?;
    tx.send(Message::Text(text)).await.context("ws send")?;
    Ok(())
}

async fn ws_recv_result(rx: &mut WsStream, expected_id: &str) -> Result<ResultMsg> {
    loop {
        match rx.next().await {
            Some(Ok(Message::Text(text))) => {
                let msg: ServerMsg = serde_json::from_str(&text)
                    .with_context(|| format!("parse WS message: {text}"))?;
                if let ServerMsg::Result(r) = msg {
                    if r.message_id == expected_id {
                        if !r.success {
                            bail!(
                                "zwave-js command {expected_id} failed: {:?}",
                                r.error_code
                            );
                        }
                        return Ok(r);
                    }
                }
            }
            Some(Ok(Message::Ping(d))) => {
                // tungstenite auto-responds to pings, but we log it
                debug!("WS ping: {} bytes", d.len());
            }
            Some(Ok(_)) => {}
            Some(Err(e)) => bail!("ws recv error: {e}"),
            None => bail!("WS stream ended during handshake"),
        }
    }
}

/// Perform the three-step handshake and return the initial node list.
async fn handshake(
    tx: &mut WsSink,
    rx: &mut WsStream,
    schema_version: u32,
) -> Result<Vec<NodeState>> {
    // Step 1: receive version announcement
    let version_msg = loop {
        match rx.next().await {
            Some(Ok(Message::Text(text))) => {
                let msg: ServerMsg = serde_json::from_str(&text)
                    .with_context(|| format!("parse version msg: {text}"))?;
                if let ServerMsg::Version(v) = msg {
                    break v;
                }
            }
            Some(Ok(_)) => {}
            Some(Err(e)) => bail!("ws error awaiting version: {e}"),
            None => bail!("WS closed before version message"),
        }
    };

    info!(
        server_version = %version_msg.server_version,
        driver_version = %version_msg.driver_version,
        min_schema = version_msg.min_schema_version,
        max_schema = version_msg.max_schema_version,
        "Connected to zwave-js-server"
    );

    let negotiated = schema_version.clamp(
        version_msg.min_schema_version,
        version_msg.max_schema_version,
    );

    // Step 2: set API schema version
    let init_id = "hc-zwave-init";
    ws_send(
        tx,
        &json!({
            "messageId": init_id,
            "command": "set_api_schema",
            "schemaVersion": negotiated,
        }),
    )
    .await?;
    ws_recv_result(rx, init_id).await?;
    info!(schema_version = negotiated, "Schema negotiated");

    // Step 3: start listening — returns full Z-Wave state
    let listen_id = "hc-zwave-listen";
    ws_send(tx, &json!({ "messageId": listen_id, "command": "start_listening" })).await?;
    let result = ws_recv_result(rx, listen_id).await?;

    let nodes = result
        .result
        .as_ref()
        .and_then(|r| r.get("state"))
        .and_then(|s| s.get("nodes"))
        .and_then(|n| n.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(NodeState::from_value)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    info!(node_count = nodes.len(), "Received initial Z-Wave state");
    Ok(nodes)
}

// ---------------------------------------------------------------------------
// Main bridge loop
// ---------------------------------------------------------------------------

/// Messages sent from the MQTT task to the WS task to trigger `node.set_value`.
#[derive(Debug)]
struct SetValueCmd {
    node_id: u32,
    command_class: u32,
    endpoint: u32,
    property: String,
    value: Value,
}

pub struct Bridge {
    pub config: Config,
}

impl Bridge {
    pub async fn run(self) -> Result<()> {
        let mut backoff_secs = 2u64;
        loop {
            match self.run_once().await {
                Ok(()) => {
                    info!("Bridge exited cleanly");
                    break;
                }
                Err(e) => {
                    error!(error = %e, backoff_secs, "Bridge error; reconnecting");
                    tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                    backoff_secs = (backoff_secs * 2).min(60);
                }
            }
        }
        Ok(())
    }

    async fn run_once(&self) -> Result<()> {
        let cfg = &self.config;

        // --- MQTT ---
        let (mqtt_client, mut eventloop) = connect_mqtt(cfg).await?;

        // Drain eventloop until we get the ConnAck so subscriptions are live.
        // Subscribe to all device cmd topics; handle_mqtt_cmd filters to zwave_* devices.
        // NOTE: MQTT '+' must occupy a full topic level — "zwave_+" is NOT valid.
        mqtt_client
            .subscribe("homecore/devices/+/cmd", QoS::AtLeastOnce)
            .await?;
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => break,
                Ok(_) => {}
                Err(e) => bail!("MQTT connect error: {e}"),
            }
        }
        info!(broker_host = %cfg.homecore.broker_host, broker_port = cfg.homecore.broker_port, "MQTT connected");

        // --- WebSocket ---
        let (ws_stream, _) = connect_async(&cfg.server.url)
            .await
            .with_context(|| format!("WS connect to {}", cfg.server.url))?;
        let (mut ws_tx, mut ws_rx) = ws_stream.split();

        // --- Handshake ---
        let translator = Translator::new();
        let nodes = handshake(&mut ws_tx, &mut ws_rx, cfg.server.schema_version).await?;

        // --- Publish initial state ---
        let plugin_id = &cfg.homecore.plugin_id;
        for node in &nodes {
            if let Err(e) = publish_node(&mqtt_client, plugin_id, node, &translator).await {
                warn!(node_id = node.node_id, error = %e, "Failed to publish initial node state");
            }
        }

        // --- Command channel: MQTT reader → WS sender ---
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<SetValueCmd>(64);

        // Spawn the WS task (owns ws_tx + ws_rx, reads cmd_rx, writes MQTT)
        let mqtt_c = mqtt_client.clone();
        let plugin_id_owned = cfg.homecore.plugin_id.clone();
        let ws_task = tokio::spawn(async move {
            ws_event_loop(&mut ws_tx, &mut ws_rx, &mut cmd_rx, &mqtt_c, &translator, &plugin_id_owned).await
        });

        // MQTT event loop in current task (reads cmd topic, sends to cmd_tx)
        let mqtt_result = mqtt_event_loop(&mut eventloop, &cmd_tx).await;
        ws_task.abort();
        mqtt_result
    }
}

// ---------------------------------------------------------------------------
// WS event loop
// ---------------------------------------------------------------------------

async fn ws_event_loop(
    ws_tx: &mut WsSink,
    ws_rx: &mut WsStream,
    cmd_rx: &mut mpsc::Receiver<SetValueCmd>,
    mqtt: &AsyncClient,
    translator: &Translator,
    plugin_id: &str,
) -> Result<()> {
    loop {
        tokio::select! {
            // Incoming WebSocket message
            frame = ws_rx.next() => {
                match frame {
                    Some(Ok(Message::Text(text))) => {
                        if let Err(e) = handle_ws_message(&text, mqtt, translator, plugin_id).await {
                            warn!(error = %e, "Error handling WS message");
                        }
                    }
                    Some(Ok(Message::Ping(_))) => {}
                    Some(Ok(Message::Close(_))) => bail!("WS closed by server"),
                    Some(Ok(_)) => {}
                    Some(Err(e)) => bail!("WS read error: {e}"),
                    None => bail!("WS stream ended"),
                }
            }
            // Outgoing command from MQTT
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(c) => {
                        if let Err(e) = send_set_value(ws_tx, &c).await {
                            warn!(error = %e, "Failed to send set_value");
                        }
                    }
                    None => break, // sender dropped
                }
            }
        }
    }
    Ok(())
}

async fn handle_ws_message(text: &str, mqtt: &AsyncClient, translator: &Translator, plugin_id: &str) -> Result<()> {
    let msg: ServerMsg = match serde_json::from_str(text) {
        Ok(m) => m,
        Err(e) => {
            debug!(error = %e, msg = %&text[..text.len().min(120)], "Unrecognised WS frame");
            return Ok(());
        }
    };

    match msg {
        ServerMsg::Event(wrapper) => handle_event(wrapper.event, mqtt, translator, plugin_id).await?,
        ServerMsg::Result(r) if !r.success => {
            warn!(
                message_id = %r.message_id,
                error_code = ?r.error_code,
                result = ?r.result,
                "zwave-js command failed"
            );
        }
        _ => {}
    }
    Ok(())
}

async fn handle_event(
    ev: crate::types::RawEvent,
    mqtt: &AsyncClient,
    translator: &Translator,
    plugin_id: &str,
) -> Result<()> {
    let node_id = match ev.node_id {
        Some(id) => id,
        None => return Ok(()),
    };
    let device_id = node_device_id(node_id);

    match ev.event.as_str() {
        "value updated" | "value added" => {
            if let Some(args_val) = ev.args {
                if let Ok(args) = serde_json::from_value::<ValueUpdatedArgs>(args_val) {
                    let pk_str = args.property_key.as_ref().and_then(property_key_str);
                    let pk = pk_str.as_deref();
                    if let Some((attr, val)) =
                        translator.translate(args.command_class, args.endpoint, &args.property, pk, &args.new_value)
                    {
                        debug!(node_id, %attr, "Value updated");
                        let patch = json!({ attr: val });
                        publish_partial(mqtt, &device_id, &patch).await?;
                    }
                }
            }
        }

        "node status changed" => {
            // args.status: 1=Asleep, 2=Awake, 3=Dead, 4=Alive
            let status = ev.args.as_ref().and_then(|a| a.get("status")).and_then(|s| s.as_u64());
            let available = matches!(status, Some(2) | Some(4));
            publish_availability(mqtt, &device_id, available).await?;
            info!(node_id, ?status, available, "Node status changed");
        }

        "node ready" => {
            if let Some(ns_val) = ev.node_state {
                if let Some(node) = NodeState::from_value(&ns_val) {
                    let display_name = node.name.as_deref().filter(|n| !n.is_empty()).unwrap_or(&device_id).to_string();
                    register_node(mqtt, plugin_id, &device_id, &display_name).await?;
                    let state = build_state(&node, translator);
                    publish_state(mqtt, &device_id, &state).await?;
                    publish_availability(mqtt, &device_id, node.is_available()).await?;
                    info!(node_id, "Node ready — published full state");
                }
            }
        }

        "node name updated" => {
            if let Some(name) = ev.name {
                register_node(mqtt, plugin_id, &device_id, &name).await?;
                let patch = json!({ "name": name });
                publish_partial(mqtt, &device_id, &patch).await?;
                debug!(node_id, %name, "Node name updated");
            }
        }

        "node location updated" => {
            if let Some(loc) = ev.location {
                let patch = json!({ "location": loc });
                publish_partial(mqtt, &device_id, &patch).await?;
                debug!(node_id, %loc, "Node location updated");
            }
        }

        "node removed" => {
            publish_availability(mqtt, &device_id, false).await?;
            info!(node_id, "Node removed");
        }

        _ => {
            debug!(event = %ev.event, node_id, "Unhandled event");
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// node.set_value
// ---------------------------------------------------------------------------

async fn send_set_value(ws_tx: &mut WsSink, cmd: &SetValueCmd) -> Result<()> {
    let msg_id = format!("hc-sv-{}", Uuid::new_v4());
    let msg = json!({
        "messageId": msg_id,
        "command": "node.set_value",
        "nodeId": cmd.node_id,
        "valueId": {
            "commandClass": cmd.command_class,
            "endpoint": cmd.endpoint,
            "property": cmd.property,
        },
        "value": cmd.value,
    });
    // Log the exact JSON so we can compare against the zwave-js-server protocol spec.
    let raw_json = serde_json::to_string(&msg).unwrap_or_default();
    info!(node_id = cmd.node_id, cc = cmd.command_class, prop = %cmd.property, value = ?cmd.value, json = %raw_json, "Sending node.set_value");
    ws_send(ws_tx, &msg).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// MQTT event loop
// ---------------------------------------------------------------------------

async fn mqtt_event_loop(
    eventloop: &mut EventLoop,
    cmd_tx: &mpsc::Sender<SetValueCmd>,
) -> Result<()> {
    let translator = Translator::new();
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::Publish(p))) => {
                handle_mqtt_cmd(&p.topic, &p.payload, &translator, cmd_tx).await;
            }
            Ok(_) => {}
            Err(e) => {
                bail!("MQTT error: {e}");
            }
        }
    }
}

async fn handle_mqtt_cmd(
    topic: &str,
    payload: &[u8],
    translator: &Translator,
    cmd_tx: &mpsc::Sender<SetValueCmd>,
) {
    // Topic: homecore/devices/zwave_{nodeId}/cmd
    let parts: Vec<&str> = topic.split('/').collect();
    if parts.len() != 4 || parts[3] != "cmd" {
        return;
    }
    let device_segment = parts[2];
    let node_id: u32 = match device_segment.strip_prefix("zwave_").and_then(|s| s.parse().ok()) {
        Some(id) => id,
        None => return, // not a zwave device — ignore
    };
    info!(node_id, "Received cmd from HomeCore");

    let cmd: Value = match serde_json::from_slice(payload) {
        Ok(v) => v,
        Err(e) => {
            warn!(topic, error = %e, "Non-JSON cmd payload");
            return;
        }
    };

    let obj = match cmd.as_object() {
        Some(o) => o,
        None => return,
    };

    for (attr, hc_value) in obj {
        let target = match translator.write_target(attr) {
            Some(t) => t,
            None => {
                debug!(attr, "No write target for attribute — ignoring");
                continue;
            }
        };
        let native_value = target.transform.reverse(hc_value);
        let sv_cmd = SetValueCmd {
            node_id,
            command_class: target.command_class,
            endpoint: target.endpoint,
            property: target.property.clone(),
            value: native_value,
        };
        if cmd_tx.send(sv_cmd).await.is_err() {
            warn!("WS task gone; dropping cmd");
        }
    }
}
