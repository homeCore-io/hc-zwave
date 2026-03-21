//! zwave-js-server WebSocket protocol types.
//!
//! The server sends JSON frames with a top-level `"type"` discriminator:
//!   - `"version"`  — initial handshake, sent on connect
//!   - `"result"`   — response to commands we send
//!   - `"event"`    — ongoing node/controller events

use serde::Deserialize;
use serde_json::Value;

// ---------------------------------------------------------------------------
// Top-level incoming message
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMsg {
    Version(VersionMsg),
    Result(ResultMsg),
    Event(EventWrapper),
}

// ---------------------------------------------------------------------------
// Version (first message on connect)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct VersionMsg {
    #[serde(rename = "driverVersion")]
    pub driver_version: String,
    #[serde(rename = "serverVersion")]
    pub server_version: String,
    #[serde(rename = "homeId")]
    pub home_id: Option<u64>,
    #[serde(rename = "minSchemaVersion")]
    pub min_schema_version: u32,
    #[serde(rename = "maxSchemaVersion")]
    pub max_schema_version: u32,
}

// ---------------------------------------------------------------------------
// Result
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct ResultMsg {
    #[serde(rename = "messageId")]
    pub message_id: String,
    pub success: bool,
    pub result: Option<Value>,
    #[serde(rename = "errorCode")]
    pub error_code: Option<String>,
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct EventWrapper {
    pub event: RawEvent,
}

/// Raw event — we keep extra fields as a flattened Value for flexibility
/// since zwave-js-server event shapes vary by event name.
#[derive(Debug, Deserialize)]
pub struct RawEvent {
    pub source: String,
    /// The event name, e.g. "value updated", "node status changed".
    pub event: String,
    #[serde(rename = "nodeId")]
    pub node_id: Option<u32>,
    /// Present on "value updated", "value added".
    pub args: Option<Value>,
    /// Present on "node ready".
    #[serde(rename = "nodeState")]
    pub node_state: Option<Value>,
    /// Present on "node name updated".
    pub name: Option<String>,
    /// Present on "node location updated".
    pub location: Option<String>,
}

// ---------------------------------------------------------------------------
// Value updated args
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct ValueUpdatedArgs {
    #[serde(rename = "commandClass")]
    pub command_class: u32,
    pub endpoint: u32,
    pub property: String,
    /// Can be null, a number, or a string depending on the CC.
    #[serde(rename = "propertyKey")]
    pub property_key: Option<Value>,
    #[serde(rename = "newValue")]
    pub new_value: Value,
}

// ---------------------------------------------------------------------------
// Node state (from start_listening result and node ready event)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct NodeState {
    #[serde(rename = "nodeId")]
    pub node_id: u32,
    pub name: Option<String>,
    pub location: Option<String>,
    /// 1=Asleep, 2=Awake, 3=Dead, 4=Alive, 5=Unknown
    pub status: Option<u8>,
    pub values: Vec<NodeValue>,
}

/// One value entry from a node's value list.
#[derive(Debug, Deserialize)]
pub struct NodeValue {
    #[serde(rename = "commandClass")]
    pub command_class: u32,
    pub endpoint: u32,
    pub property: String,
    #[serde(rename = "propertyKey")]
    pub property_key: Option<Value>,
    pub value: Option<Value>,
}

impl NodeState {
    /// Parse from the raw JSON returned by start_listening or nodeState.
    pub fn from_value(v: &Value) -> Option<Self> {
        serde_json::from_value(v.clone()).ok()
    }

    /// Whether the node is reachable (Alive or Awake).
    pub fn is_available(&self) -> bool {
        matches!(self.status, Some(2) | Some(4) | None)
    }
}
