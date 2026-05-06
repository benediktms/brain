use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// A single event in the saga event log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SagaEvent {
    pub event_id: String,
    pub saga_id: String,
    pub timestamp: i64,
    pub actor: String,
    pub event_type: SagaEventType,
    pub payload: serde_json::Value,
}

/// The set of event types for the saga subsystem.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SagaEventType {
    SagaCreated,
    SagaUpdated,
    SagaStarted,
    SagaClosed,
    SagaCancelled,
    SagaReopened,
    SagaTaskAdded,
    SagaTaskRemoved,
}

/// Payload for `SagaClosed` and `SagaCancelled` — carries the cascade flag.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SagaClosedPayload {
    pub cascade: bool,
}

/// Payload for `SagaCancelled`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SagaCancelledPayload {
    pub cascade: bool,
}

/// Payload for `SagaTaskAdded` and `SagaTaskRemoved`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SagaTaskPayload {
    pub task_id: String,
}

impl SagaEvent {
    pub fn new(
        saga_id: impl Into<String>,
        actor: impl Into<String>,
        event_type: SagaEventType,
        payload: &impl Serialize,
    ) -> Self {
        Self {
            event_id: new_saga_event_id(),
            saga_id: saga_id.into(),
            timestamp: crate::utils::now_ts(),
            actor: actor.into(),
            event_type,
            payload: serde_json::to_value(payload).unwrap(),
        }
    }
}

/// Generate a new ULID event ID for saga events.
pub fn new_saga_event_id() -> String {
    Ulid::new().to_string()
}

/// Generate a new saga ID: bare 26-char ULID with no prefix.
///
/// Sagas are registry-level (not scoped to any brain), so no prefix is
/// needed or desired — the bare ULID shape is the type signature.
pub fn new_saga_id() -> String {
    Ulid::new().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip<T: Serialize + for<'de> Deserialize<'de> + std::fmt::Debug + PartialEq>(val: &T) {
        let json = serde_json::to_string(val).expect("serialize");
        let recovered: T = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(*val, recovered);
    }

    #[test]
    fn saga_event_type_serde_all_variants() {
        round_trip(&SagaEventType::SagaCreated);
        round_trip(&SagaEventType::SagaUpdated);
        round_trip(&SagaEventType::SagaStarted);
        round_trip(&SagaEventType::SagaClosed);
        round_trip(&SagaEventType::SagaCancelled);
        round_trip(&SagaEventType::SagaReopened);
        round_trip(&SagaEventType::SagaTaskAdded);
        round_trip(&SagaEventType::SagaTaskRemoved);
    }

    #[test]
    fn saga_event_type_snake_case_names() {
        assert_eq!(
            serde_json::to_string(&SagaEventType::SagaCreated).unwrap(),
            "\"saga_created\""
        );
        assert_eq!(
            serde_json::to_string(&SagaEventType::SagaClosed).unwrap(),
            "\"saga_closed\""
        );
        assert_eq!(
            serde_json::to_string(&SagaEventType::SagaTaskAdded).unwrap(),
            "\"saga_task_added\""
        );
    }

    #[test]
    fn saga_closed_payload_round_trip() {
        let p = SagaClosedPayload { cascade: true };
        let json = serde_json::to_string(&p).unwrap();
        let back: SagaClosedPayload = serde_json::from_str(&json).unwrap();
        assert_eq!(back.cascade, true);

        let p2 = SagaClosedPayload { cascade: false };
        let json2 = serde_json::to_string(&p2).unwrap();
        let back2: SagaClosedPayload = serde_json::from_str(&json2).unwrap();
        assert_eq!(back2.cascade, false);
    }

    #[test]
    fn saga_cancelled_payload_round_trip() {
        let p = SagaCancelledPayload { cascade: true };
        let json = serde_json::to_string(&p).unwrap();
        let back: SagaCancelledPayload = serde_json::from_str(&json).unwrap();
        assert_eq!(back.cascade, true);
    }

    #[test]
    fn saga_task_payload_round_trip() {
        let p = SagaTaskPayload {
            task_id: "BRN-01JXYZ".to_string(),
        };
        let json = serde_json::to_string(&p).unwrap();
        let back: SagaTaskPayload = serde_json::from_str(&json).unwrap();
        assert_eq!(back.task_id, "BRN-01JXYZ");
    }

    #[test]
    fn new_saga_id_is_26_chars_no_prefix() {
        let id = new_saga_id();
        assert_eq!(id.len(), 26, "saga ID must be a bare 26-char ULID");
        assert!(!id.contains('-'), "saga ID must have no prefix/hyphen");
    }

    #[test]
    fn new_saga_id_is_unique() {
        let a = new_saga_id();
        let b = new_saga_id();
        assert_ne!(a, b);
    }

    #[test]
    fn saga_event_new_builds_correctly() {
        let payload = SagaClosedPayload { cascade: false };
        let ev = SagaEvent::new("saga123", "cli", SagaEventType::SagaClosed, &payload);
        assert_eq!(ev.saga_id, "saga123");
        assert_eq!(ev.actor, "cli");
        assert_eq!(ev.event_type, SagaEventType::SagaClosed);
        assert_eq!(ev.event_id.len(), 26);

        let recovered: SagaClosedPayload =
            serde_json::from_value(ev.payload).expect("payload round-trip");
        assert_eq!(recovered.cascade, false);
    }
}
