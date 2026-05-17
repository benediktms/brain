//! Channel message types exchanged between the RPC dispatcher and the
//! file-watcher supervisor.

use tokio::sync::oneshot;

/// Commands sent from the RPC dispatcher to the watcher supervisor.
pub enum ControlMessage {
    Add {
        path: String,
        reply: oneshot::Sender<Result<AddOutcome, String>>,
    },
    Remove {
        path: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
    List {
        reply: oneshot::Sender<Vec<WatchEntry>>,
    },
}

/// Result of a successful `Add` control message.
pub struct AddOutcome {
    pub brain_name: String,
}

/// A single registered watch as reported by the supervisor.
pub struct WatchEntry {
    pub brain_name: String,
    pub brain_id: String,
    pub note_dir: String,
    pub watching: bool,
}
