//! Smoke test for `brain_daemon::watcher::Supervisor::bootstrap_and_run`.
//!
//! TODO(brn-2fe-31 US-009): Re-enable once the supervisor accepts an injected
//! `Embed` impl (or a mock-embedder fixture lands). Today `bootstrap_and_run`
//! calls `Embedder::load(&model_dir)` which expects real candle weights on
//! disk under the per-brain `model_dir`. Wiring a tempdir brain through it
//! would need ~90MB of model artifacts, which isn't appropriate for a unit
//! smoke test. The full integration verification covered by US-009 will
//! exercise this path end-to-end against a real install.

#![cfg(all(unix, feature = "embed"))]

#[test]
#[ignore = "requires real embedder weights; see TODO above"]
fn supervisor_bootstrap_and_run_smoke() {
    // Intentionally empty — see the module-level TODO.
}
