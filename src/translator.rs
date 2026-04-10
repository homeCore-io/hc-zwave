//! ValueID ↔ HomeCore attribute translation.
//!
//! Maps `{commandClass}/{endpoint}/{property}[/{propertyKey}]` to a canonical
//! HomeCore attribute name plus an optional value transform.
//!
//! For writable attributes (Binary Switch, Dimmer, Lock, Thermostat), the
//! write target property (e.g. `targetValue`) is recorded separately so
//! commands are sent to the correct Z-Wave property.

use serde_json::Value;
use std::collections::HashMap;
use std::sync::OnceLock;

// ---------------------------------------------------------------------------
// Transform pipeline
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub enum Transform {
    /// Pass value through unchanged.
    Identity,
    /// Non-zero number/bool → true, zero → false.
    /// Used for Door Lock CC 98: 255 → true (locked), 0 → false.
    /// Also used for CC 113 Notification "Opening state" (1=open, 0=closed).
    NonzeroBool,
    /// CC 113 Access Control "Door state" numeric → contact_open bool.
    /// 22 = "Window/door is open" → true, 23 = "closed" / 0 = idle → false.
    AccessControlDoorState,
    /// Integer → canonical string via lookup table.
    /// Used for Thermostat Mode CC 64.
    ModeMap,
}

impl Transform {
    /// Apply forward transform: raw ZWave value → HomeCore canonical value.
    pub fn apply(&self, v: &Value) -> Value {
        match self {
            Transform::Identity => v.clone(),
            Transform::NonzeroBool => {
                let nonzero = match v {
                    Value::Bool(b) => *b,
                    Value::Number(n) => n.as_f64().map_or(false, |f| f != 0.0),
                    _ => false,
                };
                Value::Bool(nonzero)
            }
            Transform::AccessControlDoorState => {
                // 22 = "Window/door is open", 23 = "Window/door is closed", 0 = idle
                let open = v.as_u64() == Some(22);
                Value::Bool(open)
            }
            Transform::ModeMap => {
                let key = value_to_string(v);
                thermostat_mode_fwd()
                    .get(key.as_str())
                    .map(|s| Value::String(s.to_string()))
                    .unwrap_or_else(|| v.clone())
            }
        }
    }

    /// Apply inverse transform: HomeCore command value → native ZWave value.
    pub fn reverse(&self, v: &Value) -> Value {
        match self {
            Transform::Identity => v.clone(),
            Transform::NonzeroBool => {
                // true → 255, false → 0  (Door Lock secure/unsecure mode)
                match v {
                    Value::Bool(true) => Value::Number(255.into()),
                    Value::Bool(false) => Value::Number(0.into()),
                    _ => v.clone(),
                }
            }
            Transform::AccessControlDoorState => v.clone(), // read-only
            Transform::ModeMap => {
                let key = value_to_string(v);
                thermostat_mode_rev()
                    .get(key.as_str())
                    .and_then(|n| serde_json::Number::from_f64(*n as f64))
                    .map(Value::Number)
                    .unwrap_or_else(|| v.clone())
            }
        }
    }
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => v.to_string(),
    }
}

// Thermostat Mode CC 64 integer → canonical string
static THERMOSTAT_MODE_FWD_DATA: &[(&str, &str)] = &[
    ("0", "off"),
    ("1", "heat"),
    ("2", "cool"),
    ("3", "auto"),
    ("6", "fan_only"),
    ("11", "energy_heat"),
];

// Canonical string → thermostat mode integer
static THERMOSTAT_MODE_REV_DATA: &[(&str, u64)] = &[
    ("off", 0),
    ("heat", 1),
    ("cool", 2),
    ("auto", 3),
    ("fan_only", 6),
    ("energy_heat", 11),
];

static THERMOSTAT_MODE_FWD_MAP: OnceLock<HashMap<&'static str, &'static str>> = OnceLock::new();
static THERMOSTAT_MODE_REV_MAP: OnceLock<HashMap<&'static str, u64>> = OnceLock::new();

fn thermostat_mode_fwd() -> &'static HashMap<&'static str, &'static str> {
    THERMOSTAT_MODE_FWD_MAP.get_or_init(|| THERMOSTAT_MODE_FWD_DATA.iter().cloned().collect())
}

fn thermostat_mode_rev() -> &'static HashMap<&'static str, u64> {
    THERMOSTAT_MODE_REV_MAP.get_or_init(|| THERMOSTAT_MODE_REV_DATA.iter().cloned().collect())
}

// ---------------------------------------------------------------------------
// Alias table entry
// ---------------------------------------------------------------------------

struct AliasEntry {
    /// Alias key: "{cc}/{endpoint}/{property}" or "{cc}/{endpoint}/{property}/{propertyKey}"
    key: &'static str,
    attribute: &'static str,
    transform: Transform,
    /// If true, this entry is the preferred write target for the attribute.
    is_write: bool,
}

/// Full alias table — order matches zwave.toml for consistency.
static ALIAS_TABLE: &[AliasEntry] = &[
    // Binary Sensor (CC 48) — read-only
    // `property` is the BinarySensorType enum name exactly as zwave-js emits it.
    // newValue is boolean: true = triggered/open, false = clear/closed.
    AliasEntry {
        key: "48/0/Motion",
        attribute: "motion",
        transform: Transform::Identity,
        is_write: false,
    },
    AliasEntry {
        key: "48/0/Door/Window",
        attribute: "contact_open",
        transform: Transform::Identity,
        is_write: false,
    },
    AliasEntry {
        key: "48/0/Water",
        attribute: "water_detected",
        transform: Transform::Identity,
        is_write: false,
    },
    AliasEntry {
        key: "48/0/Smoke",
        attribute: "smoke",
        transform: Transform::Identity,
        is_write: false,
    },
    AliasEntry {
        key: "48/0/CO",
        attribute: "co",
        transform: Transform::Identity,
        is_write: false,
    },
    AliasEntry {
        key: "48/0/CO2",
        attribute: "co2_alarm",
        transform: Transform::Identity,
        is_write: false,
    },
    AliasEntry {
        key: "48/0/Heat",
        attribute: "heat_alarm",
        transform: Transform::Identity,
        is_write: false,
    },
    AliasEntry {
        key: "48/0/Freeze",
        attribute: "freeze",
        transform: Transform::Identity,
        is_write: false,
    },
    AliasEntry {
        key: "48/0/Tamper",
        attribute: "tamper",
        transform: Transform::Identity,
        is_write: false,
    },
    AliasEntry {
        key: "48/0/Tilt",
        attribute: "tilt",
        transform: Transform::Identity,
        is_write: false,
    },
    AliasEntry {
        key: "48/0/Glass Break",
        attribute: "glass_break",
        transform: Transform::Identity,
        is_write: false,
    },
    // CC 48 v1 devices report all sensor types as "Any" — treat as generic contact
    AliasEntry {
        key: "48/0/Any",
        attribute: "sensor_active",
        transform: Transform::Identity,
        is_write: false,
    },
    // Binary Switch (CC 37) — currentValue for read, targetValue for write
    AliasEntry {
        key: "37/0/currentValue",
        attribute: "on",
        transform: Transform::Identity,
        is_write: false,
    },
    AliasEntry {
        key: "37/0/targetValue",
        attribute: "on",
        transform: Transform::Identity,
        is_write: true,
    },
    // Multilevel Switch / Dimmer (CC 38) — brightness 0-99
    AliasEntry {
        key: "38/0/currentValue",
        attribute: "brightness",
        transform: Transform::Identity,
        is_write: false,
    },
    AliasEntry {
        key: "38/0/targetValue",
        attribute: "brightness",
        transform: Transform::Identity,
        is_write: true,
    },
    // Multilevel Sensor (CC 49) — read-only
    AliasEntry {
        key: "49/0/Air temperature",
        attribute: "temperature",
        transform: Transform::Identity,
        is_write: false,
    },
    AliasEntry {
        key: "49/0/Humidity",
        attribute: "humidity",
        transform: Transform::Identity,
        is_write: false,
    },
    AliasEntry {
        key: "49/0/Luminance",
        attribute: "illuminance",
        transform: Transform::Identity,
        is_write: false,
    },
    AliasEntry {
        key: "49/0/Ultraviolet",
        attribute: "uv_index",
        transform: Transform::Identity,
        is_write: false,
    },
    AliasEntry {
        key: "49/0/CO2 level",
        attribute: "co2_ppm",
        transform: Transform::Identity,
        is_write: false,
    },
    AliasEntry {
        key: "49/0/Atmospheric pressure",
        attribute: "pressure",
        transform: Transform::Identity,
        is_write: false,
    },
    // Meter (CC 50) — read-only, propertyKey-based
    AliasEntry {
        key: "50/0/value/65537",
        attribute: "power_w",
        transform: Transform::Identity,
        is_write: false,
    },
    AliasEntry {
        key: "50/0/value/65538",
        attribute: "energy_kwh",
        transform: Transform::Identity,
        is_write: false,
    },
    AliasEntry {
        key: "50/0/value/65539",
        attribute: "voltage",
        transform: Transform::Identity,
        is_write: false,
    },
    AliasEntry {
        key: "50/0/value/65540",
        attribute: "current_a",
        transform: Transform::Identity,
        is_write: false,
    },
    // Battery (CC 128) — read-only
    AliasEntry {
        key: "128/0/level",
        attribute: "battery",
        transform: Transform::Identity,
        is_write: false,
    },
    AliasEntry {
        key: "128/0/isLow",
        attribute: "battery_low",
        transform: Transform::Identity,
        is_write: false,
    },
    // Door Lock (CC 98)
    // DoorLockMode: 0=Unsecured, 255=Secured (plus 1,16,17,32,33 for timed/inside/outside variants)
    // currentMode is read-only (reflects what the hardware confirmed).
    // targetMode is write-only (sends DoorLockCCOperationSet, which is how zwave-js expects it).
    AliasEntry {
        key: "98/0/currentMode",
        attribute: "locked",
        transform: Transform::NonzeroBool,
        is_write: false,
    },
    AliasEntry {
        key: "98/0/targetMode",
        attribute: "locked",
        transform: Transform::NonzeroBool,
        is_write: true,
    },
    // Physical sensor feedback — only present when the lock hardware has these sensors
    AliasEntry {
        key: "98/0/boltStatus",
        attribute: "bolt_status",
        transform: Transform::Identity,
        is_write: false,
    },
    AliasEntry {
        key: "98/0/latchStatus",
        attribute: "latch_status",
        transform: Transform::Identity,
        is_write: false,
    },
    AliasEntry {
        key: "98/0/doorStatus",
        attribute: "door_status",
        transform: Transform::Identity,
        is_write: false,
    },
    // Operation type: 1=Constant (stay locked/unlocked), 2=Timed (auto-relock after timeout)
    AliasEntry {
        key: "98/0/operationType",
        attribute: "lock_operation_type",
        transform: Transform::Identity,
        is_write: true,
    },
    // Timed mode: seconds until auto-relock.  Requires operationType=2 to be active.
    AliasEntry {
        key: "98/0/lockTimeoutConfiguration",
        attribute: "lock_timeout_secs",
        transform: Transform::Identity,
        is_write: true,
    },
    AliasEntry {
        key: "98/0/autoRelockTime",
        attribute: "lock_auto_relock_secs",
        transform: Transform::Identity,
        is_write: true,
    },
    // Window Covering (CC 102)
    AliasEntry {
        key: "102/0/currentValue",
        attribute: "position",
        transform: Transform::Identity,
        is_write: false,
    },
    AliasEntry {
        key: "102/0/targetValue",
        attribute: "position",
        transform: Transform::Identity,
        is_write: true,
    },
    // Color Switch (CC 51)
    AliasEntry {
        key: "51/0/currentColor",
        attribute: "color_rgb",
        transform: Transform::Identity,
        is_write: false,
    },
    AliasEntry {
        key: "51/0/targetColor",
        attribute: "color_rgb",
        transform: Transform::Identity,
        is_write: true,
    },
    // Thermostat Setpoint (CC 67) — endpoint 1 = heating setpoint
    AliasEntry {
        key: "67/1/value",
        attribute: "target_temp",
        transform: Transform::Identity,
        is_write: true,
    },
    // Thermostat Mode (CC 64)
    AliasEntry {
        key: "64/0/mode",
        attribute: "mode",
        transform: Transform::ModeMap,
        is_write: true,
    },
    // Thermostat Operating State (CC 66) — read-only
    AliasEntry {
        key: "66/0/state",
        attribute: "hvac_action",
        transform: Transform::Identity,
        is_write: false,
    },
    // Notification (CC 113) — read-only.
    // CC 113 ALWAYS has a propertyKey (the variable name string); entries without
    // a propertyKey will never match.  Key format: "{cc}/{ep}/{property}/{propertyKey}".
    //
    // Access Control (notification type 0x06)
    // "Door state": 22 = open, 23 = closed, 0 = idle
    AliasEntry {
        key: "113/0/Access Control/Door state",
        attribute: "contact_open",
        transform: Transform::AccessControlDoorState,
        is_write: false,
    },
    // "Opening state": synthetic value created by zwave-js; 1 = open, 0 = closed
    AliasEntry {
        key: "113/0/Access Control/Opening state",
        attribute: "contact_open",
        transform: Transform::NonzeroBool,
        is_write: false,
    },
    //
    // Home Security (notification type 0x07)
    // "Motion sensor status": 0 = idle, non-zero = motion detected
    AliasEntry {
        key: "113/0/Home Security/Motion sensor status",
        attribute: "motion",
        transform: Transform::NonzeroBool,
        is_write: false,
    },
    // Tamper events (product cover removed or invalid code attempts)
    AliasEntry {
        key: "113/0/Home Security/Tampering, Product cover removed",
        attribute: "tamper",
        transform: Transform::NonzeroBool,
        is_write: false,
    },
    AliasEntry {
        key: "113/0/Home Security/Tampering, invalid code",
        attribute: "tamper",
        transform: Transform::NonzeroBool,
        is_write: false,
    },
    //
    // Smoke Alarm (notification type 0x01)
    AliasEntry {
        key: "113/0/Smoke Alarm/Smoke sensor status",
        attribute: "smoke",
        transform: Transform::NonzeroBool,
        is_write: false,
    },
    // CO Alarm (notification type 0x02)
    AliasEntry {
        key: "113/0/CO Alarm/CO sensor status",
        attribute: "co",
        transform: Transform::NonzeroBool,
        is_write: false,
    },
    // Water Alarm (notification type 0x05)
    AliasEntry {
        key: "113/0/Water Alarm/Sensor status",
        attribute: "water_detected",
        transform: Transform::NonzeroBool,
        is_write: false,
    },
];

// ---------------------------------------------------------------------------
// Write target
// ---------------------------------------------------------------------------

pub struct WriteTarget {
    pub command_class: u32,
    pub endpoint: u32,
    pub property: String,
    pub transform: Transform,
}

// ---------------------------------------------------------------------------
// Translator
// ---------------------------------------------------------------------------

pub struct Translator {
    /// Forward map: alias_key → (attribute, transform)
    forward: HashMap<String, (String, Transform)>,
    /// Reverse map: attribute → write target (cc/endpoint/property + inverse transform)
    reverse: HashMap<String, WriteTarget>,
}

impl Translator {
    pub fn new() -> Self {
        let mut forward: HashMap<String, (String, Transform)> = HashMap::new();
        let mut reverse: HashMap<String, WriteTarget> = HashMap::new();

        for entry in ALIAS_TABLE {
            // Split key to extract cc and endpoint for the reverse target
            forward
                .entry(entry.key.to_string())
                .or_insert_with(|| (entry.attribute.to_string(), entry.transform.clone()));

            if entry.is_write {
                let parts: Vec<&str> = entry.key.splitn(4, '/').collect();
                if parts.len() >= 3 {
                    let cc: u32 = parts[0].parse().unwrap_or(0);
                    let ep: u32 = parts[1].parse().unwrap_or(0);
                    let prop = parts[2].to_string();
                    reverse.insert(
                        entry.attribute.to_string(),
                        WriteTarget {
                            command_class: cc,
                            endpoint: ep,
                            property: prop,
                            transform: entry.transform.clone(),
                        },
                    );
                }
            }
        }

        Self { forward, reverse }
    }

    /// Build the alias key from a ValueID's components.
    pub fn alias_key(cc: u32, endpoint: u32, property: &str, property_key: Option<&str>) -> String {
        match property_key {
            Some(pk) => format!("{cc}/{endpoint}/{property}/{pk}"),
            None => format!("{cc}/{endpoint}/{property}"),
        }
    }

    /// Translate a raw ZWave value to `(attribute_name, canonical_value)`.
    /// Returns `None` if this ValueID is not in the alias table.
    pub fn translate(
        &self,
        cc: u32,
        endpoint: u32,
        property: &str,
        property_key: Option<&str>,
        raw_value: &Value,
    ) -> Option<(String, Value)> {
        let key = Self::alias_key(cc, endpoint, property, property_key);
        let (attr, transform) = self.forward.get(&key)?;
        Some((attr.clone(), transform.apply(raw_value)))
    }

    /// Find the write target for a HomeCore attribute.
    pub fn write_target(&self, attribute: &str) -> Option<&WriteTarget> {
        self.reverse.get(attribute)
    }
}

/// Stringify a `propertyKey` Value for use in alias keys.
/// Returns `None` if the key is null/absent (omit from key).
pub fn property_key_str(pk: &Value) -> Option<String> {
    match pk {
        Value::Null => None,
        Value::Number(n) => Some(n.to_string()),
        Value::String(s) if s.is_empty() => None,
        Value::String(s) => Some(s.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_switch_forward() {
        let t = Translator::new();
        let (attr, val) = t
            .translate(37, 0, "currentValue", None, &Value::Bool(true))
            .unwrap();
        assert_eq!(attr, "on");
        assert_eq!(val, Value::Bool(true));
    }

    #[test]
    fn door_lock_nonzero_bool() {
        let t = Translator::new();
        let (attr, val) = t
            .translate(98, 0, "currentMode", None, &Value::Number(255.into()))
            .unwrap();
        assert_eq!(attr, "locked");
        assert_eq!(val, Value::Bool(true));

        let (_, val2) = t
            .translate(98, 0, "currentMode", None, &Value::Number(0.into()))
            .unwrap();
        assert_eq!(val2, Value::Bool(false));
    }

    #[test]
    fn thermostat_mode_map() {
        let t = Translator::new();
        let (attr, val) = t
            .translate(64, 0, "mode", None, &Value::Number(1.into()))
            .unwrap();
        assert_eq!(attr, "mode");
        assert_eq!(val, Value::String("heat".into()));
    }

    #[test]
    fn meter_propertykey() {
        let t = Translator::new();
        let (attr, val) = t
            .translate(50, 0, "value", Some("65537"), &serde_json::json!(120.5))
            .unwrap();
        assert_eq!(attr, "power_w");
        assert_eq!(val, serde_json::json!(120.5));
    }

    #[test]
    fn door_lock_reverse() {
        let t = Translator::new();
        let target = t.write_target("locked").unwrap();
        assert_eq!(target.command_class, 98);
        assert_eq!(target.property, "targetMode");
        let native = target.transform.reverse(&Value::Bool(true));
        assert_eq!(native, Value::Number(255.into()));
    }

    #[test]
    fn unknown_cc_returns_none() {
        let t = Translator::new();
        assert!(t
            .translate(999, 0, "unknownProp", None, &Value::Bool(true))
            .is_none());
    }

    #[test]
    fn cc48_contact_sensor() {
        let t = Translator::new();
        // Door/Window open (true) and closed (false)
        let (attr, val) = t
            .translate(48, 0, "Door/Window", None, &Value::Bool(true))
            .unwrap();
        assert_eq!(attr, "contact_open");
        assert_eq!(val, Value::Bool(true));

        let (_, val2) = t
            .translate(48, 0, "Door/Window", None, &Value::Bool(false))
            .unwrap();
        assert_eq!(val2, Value::Bool(false));

        // Motion sensor
        let (attr3, _) = t
            .translate(48, 0, "Motion", None, &Value::Bool(true))
            .unwrap();
        assert_eq!(attr3, "motion");

        // Old "Door/Window Status" key should no longer match
        assert!(t
            .translate(48, 0, "Door/Window Status", None, &Value::Bool(true))
            .is_none());
    }

    #[test]
    fn cc113_access_control_door_state() {
        let t = Translator::new();
        // 22 = open
        let (attr, val) = t
            .translate(
                113,
                0,
                "Access Control",
                Some("Door state"),
                &Value::Number(22.into()),
            )
            .unwrap();
        assert_eq!(attr, "contact_open");
        assert_eq!(val, Value::Bool(true));

        // 23 = closed
        let (_, val2) = t
            .translate(
                113,
                0,
                "Access Control",
                Some("Door state"),
                &Value::Number(23.into()),
            )
            .unwrap();
        assert_eq!(val2, Value::Bool(false));

        // 0 = idle
        let (_, val3) = t
            .translate(
                113,
                0,
                "Access Control",
                Some("Door state"),
                &Value::Number(0.into()),
            )
            .unwrap();
        assert_eq!(val3, Value::Bool(false));
    }

    #[test]
    fn cc113_opening_state() {
        let t = Translator::new();
        // 1 = open, 0 = closed (synthetic value from zwave-js)
        let (attr, val) = t
            .translate(
                113,
                0,
                "Access Control",
                Some("Opening state"),
                &Value::Number(1.into()),
            )
            .unwrap();
        assert_eq!(attr, "contact_open");
        assert_eq!(val, Value::Bool(true));

        let (_, val2) = t
            .translate(
                113,
                0,
                "Access Control",
                Some("Opening state"),
                &Value::Number(0.into()),
            )
            .unwrap();
        assert_eq!(val2, Value::Bool(false));
    }

    #[test]
    fn cc113_no_propertykey_does_not_match() {
        // CC 113 without a propertyKey should not match anything — it always has one.
        let t = Translator::new();
        assert!(t
            .translate(113, 0, "Access Control", None, &Value::Number(22.into()))
            .is_none());
        assert!(t
            .translate(113, 0, "Home Security", None, &Value::Number(1.into()))
            .is_none());
    }
}
