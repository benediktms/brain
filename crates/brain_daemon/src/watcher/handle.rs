//! `WatcherHandle` — thin `mpsc::Sender` wrapper used by the RPC
//! dispatcher to talk to the running watcher supervisor.

use tokio::sync::mpsc;
use tokio::sync::oneshot;

use super::control::{AddOutcome, ControlMessage, WatchEntry};

/// Handle the RPC dispatcher uses to talk to the running watcher supervisor.
/// Clone-cheap (the underlying sender is `Arc`-backed). The supervisor lives
/// in a dedicated task; this just hands messages over.
#[derive(Clone)]
pub struct WatcherHandle {
    tx: mpsc::Sender<ControlMessage>,
}

impl WatcherHandle {
    pub fn new(tx: mpsc::Sender<ControlMessage>) -> Self {
        Self { tx }
    }

    pub async fn add(&self, path: String) -> Result<AddOutcome, String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(ControlMessage::Add {
                path,
                reply: reply_tx,
            })
            .await
            .map_err(|_| "supervisor channel closed".to_string())?;
        reply_rx
            .await
            .map_err(|_| "supervisor dropped reply channel".to_string())?
    }

    pub async fn remove(&self, path: String) -> Result<(), String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(ControlMessage::Remove {
                path,
                reply: reply_tx,
            })
            .await
            .map_err(|_| "supervisor channel closed".to_string())?;
        reply_rx
            .await
            .map_err(|_| "supervisor dropped reply channel".to_string())?
    }

    pub async fn list(&self) -> Result<Vec<WatchEntry>, String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(ControlMessage::List { reply: reply_tx })
            .await
            .map_err(|_| "supervisor channel closed".to_string())?;
        reply_rx
            .await
            .map_err(|_| "supervisor dropped reply channel".to_string())
    }
}
