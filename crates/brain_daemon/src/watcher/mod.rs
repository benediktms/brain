//! File-watcher supervisor — owns per-brain pipelines, routes events,
//! handles registry housekeeping.
//!
//! The supervisor is reachable from the RPC dispatcher via
//! [`WatcherHandle`] (an `mpsc::Sender<ControlMessage>` wrapper).

pub mod control;
pub mod handle;
pub mod instance;
pub mod registry;
pub mod routing;
pub mod shutdown;
pub mod supervisor;

pub use control::ControlMessage;
pub use handle::WatcherHandle;
pub use supervisor::Supervisor;
