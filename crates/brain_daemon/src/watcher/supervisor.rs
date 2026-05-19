//! `Supervisor` — the long-running watcher supervisor.
//!
//! Owns per-brain pipelines and routes file events to the right one. The
//! dispatcher talks to it via `WatcherHandle`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use brain_lib::config::paths::normalize_note_paths_lenient;
use brain_lib::config::{
    brain_home, load_global_config, resolve_brain_id, resolve_paths_for_brain,
};
use brain_lib::embedder::Embedder;
use brain_lib::ipc::router::BrainRouter;
use brain_lib::ipc::server::IpcServer;
use brain_lib::pipeline::{embed_poll, job_worker, recurring_jobs};
use brain_lib::prelude::*;
use brain_lib::watcher::ESTIMATED_PATHS_PER_DIR;
use brain_persistence::db::Db;
use brain_persistence::db::meta::generate_prefix;
use brain_persistence::db::schema::BrainProjection;
use brain_persistence::store::Store;
use tokio::signal::unix::SignalKind;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::control::{AddOutcome, ControlMessage, WatchEntry};
use super::instance::{BrainInstance, init_brain_instance};
use super::registry::{
    project_db_to_config, reload_and_project, sync_brain_ids, sync_prefixes_to_config,
    validate_roots,
};
use super::routing::{build_prefix_map, event_primary_path, lookup_brain};
use super::shutdown::{ShutdownOutcome, ShutdownReason, drain_with_timeout};

/// Initialise the BrainRouter's RPC client after the daemon socket is listening.
///
/// This must be called *after* `IpcServer::bind` because `BrainRouter` acts as
/// a client to its own socket — the socket must exist before `connect()` runs.
fn init_router_client(
    sock_path: &Path,
    router: &Arc<brain_lib::ipc::router::BrainRouter>,
) -> Result<()> {
    let transport = brain_rpc::UnixSocketTransport::connect(sock_path).map_err(|e| {
        anyhow::anyhow!(
            "failed to connect IPC transport to {}: {e}",
            sock_path.display()
        )
    })?;
    let client = brain_rpc::DaemonClient::connect(transport)
        .map_err(|e| anyhow::anyhow!("failed to hand off IPC transport to DaemonClient: {e}"))?;
    router.set_client(client);
    Ok(())
}


/// The long-running watcher supervisor. Owns per-brain pipelines and routes
/// file events to the right one. The dispatcher talks to it via WatcherHandle.
pub struct Supervisor {
    brains: HashMap<String, BrainInstance>,
    shared_db: Db,
    shared_store: Store,
    embedder: Arc<dyn Embed>,
    watcher: BrainWatcher,
    prefix_map: Vec<(PathBuf, String)>,
    control_rx: mpsc::Receiver<ControlMessage>,
}

impl Supervisor {
    /// Bootstrap shared resources (db, store, embedder), initialise per-brain
    /// pipelines, start the file watcher, and run the event loop until shutdown.
    ///
    /// `rpc_shutdown` is the handle to the RPC server's accept loop. When this
    /// supervisor exits (signal, channel close, or error), Phase 1 of the
    /// shutdown sequence flips that handle so the main thread's accept loop
    /// stops on its next poll. Callers without a co-located RPC server (e.g.
    /// the legacy `brain watch` shim) pass [`crate::ShutdownHandle::noop`].
    pub async fn bootstrap_and_run(
        control_rx: mpsc::Receiver<ControlMessage>,
        rpc_shutdown: crate::ShutdownHandle,
    ) -> Result<ShutdownOutcome> {
        // ── 1. Load global config ────────────────────────────────────────
        let global_cfg = load_global_config()?;
        if global_cfg.brains.is_empty() {
            bail!(
                "no brains are registered in the global config. \
                 Run `brain register` inside a project to add one."
            );
        }

        // ── 1b. Sync brain IDs ───────────────────────────────────────────
        sync_brain_ids(&global_cfg);

        // ── 2. Load shared resources once ────────────────────────────────
        let first_name = global_cfg.brains.keys().next().expect("non-empty map");
        let first_paths = resolve_paths_for_brain(first_name)?;
        let model_dir = first_paths.model_dir.clone();

        let sqlite_path = first_paths.sqlite_db.clone();
        let shared_db: Db = tokio::task::spawn_blocking(move || Db::open(&sqlite_path))
            .await
            .map_err(|e| anyhow::anyhow!("spawn_blocking Db::open: {e}"))??;

        let embedder: Arc<dyn Embed> = {
            let model_dir_clone = model_dir.clone();
            let loaded = tokio::task::spawn_blocking(move || Embedder::load(&model_dir_clone))
                .await
                .map_err(|e| anyhow::anyhow!("spawn_blocking embedder: {e}"))??;
            Arc::new(loaded)
        };

        // ensure_schema_version MUST run before cloning — drop_and_recreate_table
        // replaces the inner Arc<Table> and pre-rebuild clones go stale.
        let mut shared_store = Store::open_or_create(&first_paths.lance_db).await?;
        brain_lib::pipeline::ensure_schema_version(&shared_db, &mut shared_store).await?;
        shared_store.set_db(Arc::new(shared_db.clone()));

        embed_poll::self_heal_if_lance_missing(&shared_db, &shared_store).await;
        shared_store.optimizer().startup_compact().await;

        // ── 3. Initialise per-brain pipelines ────────────────────────────
        let mut brains: HashMap<String, BrainInstance> = HashMap::new();

        for (name, entry) in &global_cfg.brains {
            let brain_id = match resolve_brain_id(entry, name) {
                Ok(id) => id,
                Err(e) => {
                    warn!(brain = %name, error = %e, "failed to resolve brain ID, skipping");
                    continue;
                }
            };
            match init_brain_instance(
                name,
                entry.notes.clone(),
                Arc::clone(&embedder),
                &brain_id,
                shared_db.clone(),
                shared_store.clone(),
            )
            .await
            {
                Ok(instance) => {
                    brains.insert(name.clone(), instance);
                }
                Err(e) => {
                    warn!(brain = %name, error = %e, "failed to initialise brain, skipping");
                }
            }
        }

        if brains.is_empty() {
            bail!("all registered brains failed to initialise; daemon cannot start");
        }

        info!(brains = brains.len(), "multi-brain daemon started");

        // ── 3b. Sync brains into DB (preserving existing prefixes) ───────
        {
            let existing_prefixes: std::collections::HashMap<String, String> = shared_db
                .list_brains(false)
                .unwrap_or_default()
                .into_iter()
                .filter_map(|row| row.prefix.map(|p| (row.brain_id, p)))
                .collect();

            let projections: Vec<BrainProjection> = brains
                .iter()
                .filter_map(|(name, inst)| {
                    let bid = inst.brain_id.clone();
                    global_cfg.brains.get(name).map(|entry| {
                        let prefix = existing_prefixes
                            .get(&bid)
                            .cloned()
                            .unwrap_or_else(|| generate_prefix(name));
                        BrainProjection {
                            brain_id: bid,
                            name: name.clone(),
                            prefix,
                            roots_json: serde_json::to_string(&entry.roots)
                                .unwrap_or_else(|_| "[]".to_string()),
                            notes_json: serde_json::to_string(&entry.notes)
                                .unwrap_or_else(|_| "[]".to_string()),
                            aliases_json: serde_json::to_string(&entry.aliases)
                                .unwrap_or_else(|_| "[]".to_string()),
                            archived: entry.archived,
                        }
                    })
                })
                .collect();
            if let Err(e) = shared_db.project_config_to_brains(&projections) {
                warn!(error = %e, "failed to sync brains into DB");
            } else {
                info!(brains = projections.len(), "brain registry synced to DB");
            }

            sync_prefixes_to_config(&shared_db, &brains);
        }

        // ── 3c. Start IPC server ─────────────────────────────────────────
        let default_brain_id = brains
            .values()
            .next()
            .map(|inst| inst.brain_id.clone())
            .expect("at least one brain is initialised");

        let sock_path = brain_home()
            .map(|h| h.join("brain.sock"))
            .unwrap_or_else(|_| PathBuf::from("/tmp/brain.sock"));

        // Create a disconnected router — the socket doesn't exist yet (it gets
        // bound by IpcServer::bind below). BrainRouter connects to its own
        // socket, so the socket must be listening before connect() is called.
        let router = BrainRouter::new_disconnected(default_brain_id);

        let (ipc_cancel, ipc_inode) = match IpcServer::bind(&sock_path, router.clone()) {
            Ok(server) => {
                let token = server.cancellation_token();
                let inode = {
                    use std::os::unix::fs::MetadataExt;
                    std::fs::metadata(&sock_path).map(|m| m.ino()).unwrap_or(0)
                };
                // Now that the socket is listening, initialize the router's
                // RPC client so IPC connections can be dispatched.
                if let Err(e) = init_router_client(&sock_path, &router) {
                    warn!(error = %e, "failed to connect IPC router to daemon socket; continuing without IPC");
                }
                tokio::spawn(async move { server.run().await });
                info!(path = ?sock_path, "IPC server started");
                (Some(token), inode)
            }
            Err(e) => {
                warn!(error = %e, "failed to start IPC server; continuing without IPC");
                (None, 0)
            }
        };

        for instance in brains.values() {
            embed_poll::self_heal_if_lance_missing(
                instance.pipeline.embedding_resetter(),
                instance.pipeline.store(),
            )
            .await;
        }

        // ── 4. Build path-to-brain lookup (longest prefix first) ─────────
        let prefix_map = build_prefix_map(&brains);

        // ── 5. Set up single BrainWatcher ────────────────────────────────
        let (tx, rx) = tokio::sync::mpsc::channel(256);

        let all_dirs: Vec<(String, PathBuf)> = brains
            .values()
            .flat_map(|inst| {
                inst.note_dirs
                    .iter()
                    .map(move |d| (inst.name.clone(), d.clone()))
            })
            .collect();

        let capacity = all_dirs.len().saturating_mul(ESTIMATED_PATHS_PER_DIR);
        let mut watcher = BrainWatcher::new_empty_with_capacity(capacity, tx)?;

        for (brain_name, dir) in &all_dirs {
            if let Err(e) = watcher.watch_path(dir) {
                warn!(brain = %brain_name, dir = %dir.display(), error = %e, "failed to watch directory");
            }
        }

        // ── 5b. Separate notify watcher for state_projection.toml ─────────
        // `config_rx` is always bound so it's in scope for `run_loop`, but
        // under `no-default-features` the sender is never used (the watcher
        // block below is cfg-gated and `config_rx` will never receive).
        let (config_tx, config_rx) = tokio::sync::mpsc::channel::<()>(4);

        #[cfg(feature = "embed")]
        let _config_watcher = {
            let projection_path = brain_home()?.join(brain_lib::config::PROJECTION_FILENAME);
            let projection_dir = projection_path
                .parent()
                .ok_or_else(|| anyhow::anyhow!("projection path has no parent directory"))?
                .to_path_buf();
            let projection_file = projection_path.file_name().unwrap().to_owned();

            {
                let config_tx = config_tx.clone();
                notify_debouncer_full::new_debouncer(
                    Duration::from_millis(500),
                    None,
                    move |result: notify_debouncer_full::DebounceEventResult| {
                        if let Ok(events) = result {
                            let is_projection = events
                                .iter()
                                .any(|e| e.paths.iter().any(|p| p.file_name() == Some(&projection_file)));
                            if is_projection {
                                let _ = config_tx.blocking_send(());
                            }
                        }
                    },
                )
                .and_then(|mut w| {
                    w.watch(
                        &projection_dir,
                        notify_debouncer_full::notify::RecursiveMode::NonRecursive,
                    )?;
                    info!(path = %projection_dir.display(), "watching state_projection.toml for changes");
                    Ok(w)
                })
                .map_err(|e| warn!(error = %e, "failed to watch state_projection.toml; changes won't auto-reload"))
                .ok()
            }
        };

        // ── 6. Signal handlers ───────────────────────────────────────────
        let sigterm = tokio::signal::unix::signal(SignalKind::terminate())
            .expect("failed to register SIGTERM handler");
        let sighup = tokio::signal::unix::signal(SignalKind::hangup())
            .expect("failed to register SIGHUP handler");
        let sigusr1 = tokio::signal::unix::signal(SignalKind::user_defined1())
            .expect("failed to register SIGUSR1 handler");

        // ── 7. Build Self + run event loop ───────────────────────────────
        let mut supervisor = Self {
            brains,
            shared_db,
            shared_store,
            embedder,
            watcher,
            prefix_map,
            control_rx,
        };

        let (shutdown_reason, mut rx) = supervisor
            .run_loop(rx, config_rx, sigterm, sighup, sigusr1)
            .await;

        // ── 8. Shutdown sequence ─────────────────────────────────────────

        // Phase 1: Stop IPC server and remove socket file FIRST.
        info!("shutdown phase 1/5: stopping IPC server");
        if let Some(token) = ipc_cancel {
            token.cancel();
        }
        if ipc_inode != 0 {
            brain_lib::ipc::server::remove_socket_if_owned(&sock_path, ipc_inode);
        }
        // Flip the RPC server's accept-loop shutdown flag so the main
        // thread exits within ACCEPT_POLL_INTERVAL. Without this the
        // main thread keeps spinning on listener.accept() forever when
        // SIGTERM only reaches this supervisor's tokio runtime, and the
        // post-supervisor cleanup in `entry.rs` is never reached.
        rpc_shutdown.request();

        // Phase 2: Stop watcher
        info!("shutdown phase 2/5: stopping file watcher");
        let Supervisor {
            mut brains,
            shared_store,
            watcher,
            prefix_map,
            ..
        } = supervisor;
        drop(watcher);

        // Phase 3: Drain pending work queues (signal shutdown only)
        let mut total_dropped: usize = 0;
        let mut force_shutdown = false;

        if matches!(shutdown_reason, ShutdownReason::Signal) {
            info!("shutdown phase 3/5: draining pending work queues (10s timeout)");

            while let Ok(evt) = rx.try_recv() {
                let event_path = event_primary_path(&evt);
                if let Some(brain_name) = lookup_brain(&prefix_map, &event_path)
                    && let Some(instance) = brains.get_mut(&brain_name)
                {
                    instance.work_queue.push(evt);
                }
            }

            for instance in brains.values_mut() {
                if instance.work_queue.is_empty() {
                    continue;
                }
                let queued = instance.work_queue.len();
                info!(brain = %instance.name, queued, "draining remaining work queue");

                let drain_result = tokio::select! {
                    result = drain_with_timeout(&instance.pipeline, &mut instance.work_queue, Duration::from_secs(10)) => {
                        result
                    }
                    _ = tokio::signal::ctrl_c() => {
                        info!("received second Ctrl+C, force-shutting down");
                        force_shutdown = true;
                        Err(instance.work_queue.len())
                    }
                };

                match drain_result {
                    Ok(processed) => {
                        info!(brain = %instance.name, processed, "drain complete");
                    }
                    Err(remaining) => {
                        total_dropped += remaining;
                        warn!(brain = %instance.name, remaining, "drain incomplete, items dropped");
                    }
                }

                if force_shutdown {
                    break;
                }
            }
        } else {
            info!("shutdown phase 3/5: channel closed, skipping drain");
        }

        if !force_shutdown {
            // Phase 4: SQLite WAL checkpoint for all brains
            info!("shutdown phase 4/5: checkpointing SQLite WAL");
            for instance in brains.values() {
                if let Err(e) = instance.pipeline.wal_checkpoint() {
                    warn!(brain = %instance.name, error = %e, "WAL checkpoint failed");
                }
            }

            // Phase 5: LanceDB optimize (single shared store)
            info!("shutdown phase 5/5: optimizing LanceDB");
            shared_store.optimizer().force_optimize().await;
        } else {
            info!("shutdown phases 4-5: skipped (force shutdown)");
        }

        let clean = !force_shutdown && total_dropped == 0;
        info!(clean, dropped_items = total_dropped, "shutdown complete");

        Ok(ShutdownOutcome {
            clean,
            dropped_items: total_dropped,
        })
    }

    /// Core event loop. Returns `(reason, rx)` so the shutdown sequence can
    /// drain any straggler events the loop didn't dispatch.
    async fn run_loop(
        &mut self,
        mut rx: mpsc::Receiver<FileEvent>,
        mut config_rx: mpsc::Receiver<()>,
        mut sigterm: tokio::signal::unix::Signal,
        mut sighup: tokio::signal::unix::Signal,
        mut sigusr1: tokio::signal::unix::Signal,
    ) -> (ShutdownReason, mpsc::Receiver<FileEvent>) {
        let mut optimize_tick = tokio::time::interval(Duration::from_secs(60));
        let mut summarize_poll_interval = tokio::time::interval(Duration::from_secs(60));
        let active_jobs = job_worker::ActiveJobs::new(job_worker::DEFAULT_MAX_CONCURRENT_JOBS);
        let mut root_validation_tick = tokio::time::interval(Duration::from_secs(60));

        let reason = loop {
            // Top-of-iteration metrics polling. The optimizer state is
            // shared across all brains (owned by shared_store), so one
            // read fans out to every per-brain Metrics object.
            {
                let pending = self.shared_store.optimizer().pending_count();
                let failures = self.shared_store.optimizer().optimize_failure_count();
                let depth = rx.len() as u64;
                for instance in self.brains.values() {
                    instance.pipeline.metrics().set_queue_depth(depth);
                    instance
                        .pipeline
                        .metrics()
                        .set_lancedb_unoptimized_rows(pending);
                    instance
                        .pipeline
                        .metrics()
                        .set_lancedb_optimize_failures(failures);
                }
            }

            tokio::select! {
                event = rx.recv() => {
                    match event {
                        Some(first) => {
                            let mut batch = vec![first];
                            let deadline = tokio::time::Instant::now() + Duration::from_millis(50);
                            while let Ok(Some(evt)) = tokio::time::timeout_at(deadline, rx.recv()).await {
                                batch.push(evt);
                            }

                            for evt in batch {
                                let event_path = event_primary_path(&evt);
                                if let Some(brain_name) = lookup_brain(&self.prefix_map, &event_path) {
                                    if let Some(instance) = self.brains.get_mut(&brain_name) {
                                        instance.work_queue.push(evt);
                                    }
                                } else {
                                    debug!(path = %event_path.display(), "event path matches no brain, dropping");
                                }
                            }

                            for instance in self.brains.values_mut() {
                                if instance.work_queue.is_empty() {
                                    continue;
                                }
                                let (renames, index_paths, delete_paths) = instance.work_queue.drain_batch();

                                for (from, to) in &renames {
                                    if let Err(e) = instance.pipeline.rename_file(from, to).await {
                                        warn!(brain = %instance.name, error = %e, "error handling rename");
                                    }
                                }
                                for p in &delete_paths {
                                    if let Err(e) = instance.pipeline.delete_file(p).await {
                                        warn!(brain = %instance.name, error = %e, "error handling delete");
                                    }
                                }
                                if !index_paths.is_empty()
                                    && let Err(e) =
                                        instance.pipeline.index_files_batch(&index_paths).await
                                {
                                    warn!(brain = %instance.name, error = %e, "error in batch index");
                                }

                                instance.pipeline.store().optimizer().maybe_optimize().await;
                            }
                        }
                        None => {
                            info!("watcher channel closed, shutting down");
                            break ShutdownReason::ChannelClosed;
                        }
                    }
                }
                _ = optimize_tick.tick() => {
                    for instance in self.brains.values() {
                        instance.pipeline.store().optimizer().maybe_optimize().await;
                    }
                }
                _ = summarize_poll_interval.tick() => {
                    for instance in self.brains.values() {
                        let brain_infos = vec![recurring_jobs::BrainInfo {
                            brain_id: instance.brain_id.clone(),
                        }];
                        if let Err(e) = recurring_jobs::reconcile_recurring_jobs(instance.pipeline.job_queue(), &brain_infos) {
                            tracing::warn!(brain = %instance.name, error = %e, "reconcile_recurring_jobs failed");
                        }

                        if let Err(e) = job_worker::reap_stuck_jobs_filtered(instance.pipeline.job_queue(), &active_jobs) {
                            tracing::warn!(brain = %instance.name, error = %e, "reap_stuck_jobs failed");
                        }
                        let pipeline_db = instance.pipeline.clone_db_for_spawn();
                        let n = job_worker::process_jobs(
                            &pipeline_db,
                            instance.pipeline.store(),
                            instance.pipeline.embedder(),
                            &active_jobs,
                            20,
                        ).await;
                        if n > 0 {
                            info!(brain = %instance.name, processed = n, "jobs dispatched");
                        }
                        let protected = recurring_jobs::protected_kinds();
                        if let Err(e) = instance.pipeline.gc_completed_jobs(7 * 86400, &protected) {
                            tracing::warn!(brain = %instance.name, error = %e, "gc_completed_jobs failed");
                        }
                    }
                }
                _ = root_validation_tick.tick() => {
                    if validate_roots(&mut self.brains, &mut self.watcher, &self.shared_db) {
                        self.prefix_map = build_prefix_map(&self.brains);
                    }
                }
                _ = config_rx.recv() => {
                    info!("state_projection.toml changed, re-projecting from DB");
                    project_db_to_config(&self.shared_db);
                }
                _ = sighup.recv() => {
                    info!("received SIGHUP, reloading brain registry");
                    match reload_and_project(&mut self.brains, &mut self.watcher, Arc::clone(&self.embedder), &self.shared_db, &self.shared_store).await {
                        Ok(()) => {
                            self.prefix_map = build_prefix_map(&self.brains);
                            info!(brains = self.brains.len(), "brain registry reloaded");
                        }
                        Err(e) => {
                            warn!(error = %e, "brain registry reload failed, continuing with existing config");
                        }
                    }
                }
                msg = self.control_rx.recv() => {
                    match msg {
                        Some(msg) => self.handle_control(msg).await,
                        None => {
                            info!("control channel closed, shutting down");
                            break ShutdownReason::ChannelClosed;
                        }
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("received Ctrl+C, shutting down");
                    break ShutdownReason::Signal;
                }
                _ = sigterm.recv() => {
                    info!("received SIGTERM, shutting down");
                    break ShutdownReason::Signal;
                }
                _ = sigusr1.recv() => {
                    let total_brains = self.brains.len();
                    eprintln!("multi-brain daemon: {} brain(s) active", total_brains);
                }
            }
        };

        (reason, rx)
    }

    /// Dispatch a single control message from the RPC layer.
    async fn handle_control(&mut self, msg: ControlMessage) {
        match msg {
            ControlMessage::Add { path, reply } => {
                let outcome = self.handle_add(&path).await;
                let _ = reply.send(outcome);
            }
            ControlMessage::Remove { path, reply } => {
                let outcome = self.handle_remove(&path).await;
                let _ = reply.send(outcome);
            }
            ControlMessage::List { reply } => {
                let _ = reply.send(self.list_watches());
            }
        }
    }

    /// Re-read state_projection.toml, find the owning brain, watch the path,
    /// rebuild the prefix map. Races with the validate_roots tick: the config
    /// reload here is the canonical view at message-receive time.
    async fn handle_add(&mut self, path_str: &str) -> Result<AddOutcome, String> {
        let raw = PathBuf::from(path_str);
        let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/"));
        let normalized = normalize_note_paths_lenient(&[raw], &cwd);
        let Some(path) = normalized.into_iter().next() else {
            return Err(format!("could not normalise path: {path_str}"));
        };

        let cfg = load_global_config().map_err(|e| format!("failed to load config: {e}"))?;

        let owner = find_owning_brain(&cfg, &path);
        let (brain_name, entry) = match owner {
            Some(pair) => pair,
            None => {
                return Err(format!(
                    "path {} is not under any registered brain root",
                    path.display()
                ));
            }
        };

        if entry.archived {
            return Err(format!(
                "brain {brain_name} is archived; run `brain unarchive` first"
            ));
        }

        let instance = match self.brains.get_mut(&brain_name) {
            Some(i) => i,
            None => {
                return Err(format!(
                    "brain {brain_name} is in config but not initialised; SIGHUP daemon first"
                ));
            }
        };

        if let Err(e) = self.watcher.watch_path(&path) {
            return Err(format!("watch_path failed: {e}"));
        }
        if !instance.note_dirs.contains(&path) {
            instance.note_dirs.push(path);
        }

        self.prefix_map = build_prefix_map(&self.brains);
        Ok(AddOutcome { brain_name })
    }

    async fn handle_remove(&mut self, path_str: &str) -> Result<(), String> {
        let raw = PathBuf::from(path_str);
        let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/"));
        let normalized = normalize_note_paths_lenient(&[raw], &cwd);
        let Some(path) = normalized.into_iter().next() else {
            return Err(format!("could not normalise path: {path_str}"));
        };

        let mut removed = false;
        for instance in self.brains.values_mut() {
            if let Some(pos) = instance.note_dirs.iter().position(|d| d == &path) {
                if let Err(e) = self.watcher.unwatch_path(&path) {
                    warn!(dir = %path.display(), error = %e, "unwatch_path failed");
                }
                instance.note_dirs.remove(pos);
                removed = true;
                break;
            }
        }

        if !removed {
            return Err(format!(
                "path {} is not currently being watched",
                path.display()
            ));
        }

        self.prefix_map = build_prefix_map(&self.brains);
        Ok(())
    }

    fn list_watches(&self) -> Vec<WatchEntry> {
        self.brains
            .values()
            .flat_map(|inst| {
                let brain_id = inst.brain_id.clone();
                let name = inst.name.clone();
                inst.note_dirs.iter().map(move |dir| WatchEntry {
                    brain_name: name.clone(),
                    brain_id: brain_id.clone(),
                    note_dir: dir.display().to_string(),
                    watching: true,
                })
            })
            .collect()
    }
}

/// Find the brain whose roots or notes contain `path` as a prefix. Returns the
/// brain name and a snapshot of its config entry.
fn find_owning_brain(
    cfg: &brain_lib::config::GlobalConfig,
    path: &Path,
) -> Option<(String, brain_lib::config::BrainEntry)> {
    let mut best: Option<(usize, String, brain_lib::config::BrainEntry)> = None;
    for (name, entry) in &cfg.brains {
        let candidates = entry.roots.iter().chain(entry.notes.iter());
        for candidate in candidates {
            if path.starts_with(candidate) {
                let depth = candidate.components().count();
                let improves = best
                    .as_ref()
                    .map(|(prev, _, _)| depth > *prev)
                    .unwrap_or(true);
                if improves {
                    best = Some((depth, name.clone(), entry.clone()));
                }
            }
        }
    }
    best.map(|(_, name, entry)| (name, entry))
}
