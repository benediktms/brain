//! Transitional re-export. The event-sourcing projection state machine is
//! domain logic and migrates into this crate in a follow-up. Do not add new
//! code here — extend the upstream module until the move lands, then this
//! file becomes the canonical home.

pub use brain_persistence::db::tasks::projections::*;
