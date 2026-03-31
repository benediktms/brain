use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use brain_lib::config::paths::normalize_note_paths_lenient;
use brain_lib::config::{
    brain_home, get_or_generate_brain_id, load_global_config, resolve_brain_id,
    resolve_paths_for_brain, save_global_config,
};
use brain_lib::embedder::Embedder;
use brain_lib::ipc::router::BrainRouter;
use brain_lib::ipc::server::IpcServer;
use brain_lib::mcp::McpContext;
use brain_lib::pipeline::IndexPipeline;
use brain_lib::pipeline::embed_poll;
use brain_lib::pipeline::job_worker;
use brain_lib::pipeline::recurring_jobs;
use brain_lib::ports::JobQueue;
use brain_lib::prelude::*;
use brain_persistence::db::Db;
use brain_persistence::store::Store;
use tracing::{debug, info, warn};

// The daemon (daemon.rs) uses libc and sends SIGTERM — unix-only.
use tokio::signal::unix::SignalKind;

use brain_persistence::db::meta::generate_prefix;
use brain_persistence::db::schema::BrainProjection;

/// Outcome of the watch shutdown sequence.
#[allow(dead_code)]
pub struct ShutdownOutcome {
    /// Whether shutdown completed all phases cleanly.
    pub clean: bool,
    /// Number of work-queue items that were not processed.
    pub dropped_items: usize,
}

/// Why the event loop exited.
enum ShutdownReason {
    /// The watcher channel closed (watcher dropped or errored).
    ChannelClosed,
    /// Received SIGINT (Ctrl+C) or SIGTERM.
    Signal,
}

/// Watch a directory for changes and re-index incrementally.
pub async fn run(
    notes_path: PathBuf,
    model_dir: PathBuf,
    db_path: PathBuf,
    sqlite_path: PathBuf,
) -> Result<ShutdownOutcome> {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/"));
    let note_dirs = normalize_note_paths_lenient(&[notes_path], &cwd);

    info!("starting brain watch on {:?}", note_dirs);

    let mut pipeline = IndexPipeline::new(&model_dir, &db_path, &sqlite_path).await?;

    // Resolve LLM provider: env vars first, then DB-backed credentials.
    let brain_home = brain_lib::config::brain_home().unwrap_or_else(|_| PathBuf::from("."));
    if let Some(provider) = brain_lib::llm::resolve_provider_with_db(pipeline.db(), &brain_home) {
        pipeline.set_summarizer(Arc::from(provider));
    }

    // Startup self-heal: if LanceDB is missing, reset embedded_at so all
    // tasks and chunks will be re-embedded on the next EmbedPollSweep job.
    embed_poll::self_heal_if_lance_missing(pipeline.db(), pipeline.store()).await;

    // Set up file watcher
    let (tx, mut rx) = tokio::sync::mpsc::channel(256);
    let _watcher = brain_lib::watcher::BrainWatcher::new(&note_dirs, tx)?;

    info!("watching for changes... (press Ctrl+C to stop)");

    // Work queue with file_id coalescing and bounded capacity.
    let mut work_queue = WorkQueue::default();

    // Periodic tick to check time-elapsed optimize trigger during quiet periods.
    // The check is near-free (two atomic loads + one mutex read); the 5min
    // threshold is evaluated inside should_optimize().
    let mut optimize_tick = tokio::time::interval(Duration::from_secs(60));

    // Embedding is now handled by the EmbedPollSweep recurring job.

    // Summarization job poll: reaps stuck jobs, processes ready jobs, GC's old ones.
    let mut summarize_poll_interval = tokio::time::interval(Duration::from_secs(30));

    // In-memory lock set: prevents the reaper from resetting jobs that are
    // still actively running in a tokio::spawn task.
    let active_jobs = job_worker::ActiveJobs::new(job_worker::DEFAULT_MAX_CONCURRENT_JOBS);

    // SIGTERM handler — the daemon sends SIGTERM on `brain stop` (daemon.rs:75)
    let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate())
        .expect("failed to register SIGTERM handler");

    // SIGUSR1 handler — dump metrics snapshot to stderr
    let mut sigusr1 = tokio::signal::unix::signal(SignalKind::user_defined1())
        .expect("failed to register SIGUSR1 handler");

    // Event loop: batch-drain, coalesce, and dispatch
    let shutdown_reason = loop {
        // Update queue depth + lancedb pending rows at top of each iteration
        pipeline.metrics().set_queue_depth(rx.len() as u64);
        pipeline
            .metrics()
            .set_lancedb_unoptimized_rows(pipeline.store().optimizer().pending_count());

        tokio::select! {
            event = rx.recv() => {
                match event {
                    Some(first) => {
                        // 1. Drain: collect first + any ready events (50ms window)
                        work_queue.push(first);
                        let deadline = tokio::time::Instant::now() + Duration::from_millis(50);
                        while let Ok(Some(evt)) = tokio::time::timeout_at(deadline, rx.recv()).await {
                            work_queue.push(evt);
                        }

                        // 2. Coalesce via work queue (deduped, bounded)
                        let (renames, index_paths, delete_paths) = work_queue.drain_batch();

                        // 3. Process renames
                        for (from, to) in &renames {
                            if let Err(e) = pipeline.rename_file(from, to).await {
                                tracing::warn!(error = %e, "error handling rename");
                            }
                        }

                        // 4. Process deletes
                        for p in &delete_paths {
                            if let Err(e) = pipeline.delete_file(p).await {
                                tracing::warn!(error = %e, "error handling delete");
                            }
                        }

                        // 5. Batch-index all changed/created files
                        if !index_paths.is_empty()
                            && let Err(e) = pipeline.index_files_batch(&index_paths).await
                        {
                            tracing::warn!(error = %e, "error in batch index");
                        }

                        // 6. Check row-count trigger after each batch
                        pipeline.store().optimizer().maybe_optimize().await;
                    }
                    None => {
                        info!("watcher channel closed, shutting down");
                        break ShutdownReason::ChannelClosed;
                    }
                }
            }
            _ = optimize_tick.tick() => {
                // Check time-elapsed trigger during quiet periods
                pipeline.store().optimizer().maybe_optimize().await;
            }
            _ = summarize_poll_interval.tick() => {
                // Reconcile recurring singleton jobs (idempotent).
                let brain_infos = vec![recurring_jobs::BrainInfo {
                    brain_id: String::new(),
                }];
                if let Err(e) = recurring_jobs::reconcile_recurring_jobs(pipeline.db(), &brain_infos) {
                    tracing::warn!(error = %e, "reconcile_recurring_jobs failed");
                }

                if let Err(e) = job_worker::reap_stuck_jobs_filtered(pipeline.db(), &active_jobs) {
                    tracing::warn!(error = %e, "reap_stuck_jobs failed");
                }
                let n = job_worker::process_jobs(
                    pipeline.db(),
                    pipeline.store(),
                    pipeline.embedder(),
                    &active_jobs,
                    20,
                ).await;
                if n > 0 {
                    info!(processed = n, "jobs dispatched");
                }
                let protected = recurring_jobs::protected_kinds();
                if let Err(e) = pipeline.db().gc_completed_jobs(7 * 86400, &protected) {
                    tracing::warn!(error = %e, "gc_completed_jobs failed");
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
                let mut snapshot = pipeline.metrics().snapshot();
                // Enrich with stuck-file count via brain_lib's Db wrapper
                let stuck_files = pipeline.db()
                    .with_read_conn(brain_persistence::db::files::find_stuck_files)
                    .unwrap_or_default();
                snapshot.dual_store_stuck_files = stuck_files.len() as u64;
                eprintln!("{}", serde_json::to_string_pretty(&snapshot).unwrap_or_default());
            }
        }
    };

    // ── Shutdown sequence ──────────────────────────────────────────

    // Phase 1: Stop watcher — no new events enter the channel.
    info!("shutdown phase 1/5: stopping file watcher");
    drop(_watcher);

    // Phase 2: Drain pending work (signal shutdown only).
    let mut dropped_items: usize = 0;
    let mut force_shutdown = false;

    if matches!(shutdown_reason, ShutdownReason::Signal) {
        info!("shutdown phase 2/5: draining pending work queue (10s timeout)");

        // Collect any remaining channel events into the work queue.
        while let Ok(evt) = rx.try_recv() {
            work_queue.push(evt);
        }

        if !work_queue.is_empty() {
            let queued = work_queue.len();
            info!(queued, "processing remaining queued items");

            // Race drain processing against a second Ctrl+C for force-shutdown.
            let drain_result = tokio::select! {
                result = drain_with_timeout(&pipeline, &mut work_queue, Duration::from_secs(10)) => {
                    result
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("received second Ctrl+C, force-shutting down");
                    force_shutdown = true;
                    Err(work_queue.len())
                }
            };

            match drain_result {
                Ok(processed) => {
                    info!(processed, "drain complete");
                }
                Err(remaining) => {
                    dropped_items = remaining;
                    tracing::warn!(dropped_items, "drain incomplete, items dropped");
                }
            }
        } else {
            info!("no pending items to drain");
        }
    } else {
        info!("shutdown phase 2/5: channel closed, skipping drain");
    }

    if !force_shutdown {
        // Phase 3: SQLite WAL checkpoint.
        info!("shutdown phase 3/5: checkpointing SQLite WAL");
        if let Err(e) = pipeline.db().wal_checkpoint() {
            tracing::warn!(error = %e, "WAL checkpoint failed");
        }

        // Phase 4: LanceDB optimize.
        info!("shutdown phase 4/5: optimizing LanceDB");
        pipeline.store().optimizer().force_optimize().await;
    } else {
        info!("shutdown phases 3-5: skipped (force shutdown)");
    }

    // Phase 5: Done.
    let clean = !force_shutdown && dropped_items == 0;
    info!(
        clean,
        dropped_items, "shutdown phase 5/5: shutdown complete"
    );

    Ok(ShutdownOutcome {
        clean,
        dropped_items,
    })
}

/// Drain remaining work-queue items through the pipeline within a timeout.
///
/// Returns `Ok(processed_count)` on success, or `Err(remaining_count)` if the
/// timeout expires before all items are processed.
async fn drain_with_timeout(
    pipeline: &IndexPipeline,
    work_queue: &mut WorkQueue,
    timeout: Duration,
) -> std::result::Result<usize, usize> {
    let result = tokio::time::timeout(timeout, async {
        let (renames, index_paths, delete_paths) = work_queue.drain_batch();
        let mut processed = 0;

        for (from, to) in &renames {
            if let Err(e) = pipeline.rename_file(from, to).await {
                tracing::warn!(error = %e, "error handling rename during drain");
            }
            processed += 1;
        }

        for p in &delete_paths {
            if let Err(e) = pipeline.delete_file(p).await {
                tracing::warn!(error = %e, "error handling delete during drain");
            }
            processed += 1;
        }

        if !index_paths.is_empty() {
            if let Err(e) = pipeline.index_files_batch(&index_paths).await {
                tracing::warn!(error = %e, "error in batch index during drain");
            }
            processed += index_paths.len();
        }

        processed
    })
    .await;

    match result {
        Ok(processed) => Ok(processed),
        Err(_) => Err(work_queue.len()),
    }
}

// ── Multi-brain support ────────────────────────────────────────────────────

/// Per-brain state held by the multi-brain event loop.
struct BrainInstance {
    name: String,
    pipeline: IndexPipeline,
    work_queue: WorkQueue,
    note_dirs: Vec<PathBuf>,
    mcp_context: Arc<McpContext>,
}

/// Watch all registered brains from the global config for changes and
/// re-index incrementally.
///
/// Reads `~/.brain/state_projection.toml`, creates a separate [`IndexPipeline`] for
/// each registered brain (sharing a single embedder and a single `Db` handle),
/// and routes file events to the correct pipeline via longest-prefix matching.
/// Handles SIGHUP to reload the brain registry without restarting.
pub async fn run_multi() -> Result<ShutdownOutcome> {
    // ── 1. Load global config ────────────────────────────────────────────
    let global_cfg = load_global_config()?;
    if global_cfg.brains.is_empty() {
        bail!(
            "no brains are registered in the global config. \
             Run `brain register` inside a project to add one."
        );
    }

    // ── 1b. Sync brain IDs ─────────────────────────────────────────────
    // Ensure every registered brain has an ID in both brain.toml and the
    // global registry. Best-effort: failures are logged but not fatal.
    sync_brain_ids(&global_cfg);

    // ── 2. Load shared resources once ────────────────────────────────────
    // All brains share a single Db handle (unified ~/.brain/brain.db),
    // a single embedder, and a single unified LanceDB store.
    // Db and Store are Clone (Arc-backed) — cloning shares the same pool.
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

    // Open the unified LanceDB store once. ensure_schema_version MUST run
    // before cloning — drop_and_recreate_table replaces the inner Arc<Table>
    // and clones made before the rebuild would hold stale references.
    let mut shared_store = Store::open_or_create(&first_paths.lance_db).await?;
    brain_lib::pipeline::ensure_schema_version(&shared_db, &mut shared_store).await?;
    shared_store.set_db(Arc::new(shared_db.clone()));

    // Self-heal: reset embedded_at if LanceDB is empty so EmbedPollSweep
    // will re-embed everything.
    embed_poll::self_heal_if_lance_missing(&shared_db, &shared_store).await;

    // ── 3. Initialise per-brain pipelines ────────────────────────────────
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

    // ── 3b. Sync brains into DB (preserving existing prefixes) ─────────
    // Upserts all registered brains into the SQL brains table. Existing
    // prefixes (e.g. set via `brain config set prefix`) are preserved via
    // COALESCE — generate_prefix() is only used as fallback for new brains.
    {
        let shared_ctx = &brains
            .values()
            .next()
            .expect("at least one brain")
            .mcp_context;
        let shared_db = shared_ctx.stores.db();

        // Read existing prefixes from DB to preserve manual overrides.
        let existing_prefixes: std::collections::HashMap<String, String> = shared_db
            .list_brains(false)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|row| row.prefix.map(|p| (row.brain_id, p)))
            .collect();

        let projections: Vec<BrainProjection> = brains
            .iter()
            .filter_map(|(name, inst)| {
                let bid = inst.mcp_context.brain_id().to_string();
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

        // Sync prefixes from DB back to state_projection.toml (read-only projection).
        sync_prefixes_to_config(shared_db, &brains);
    }

    // ── 3c. Start IPC server ─────────────────────────────────────────────
    // Use the first brain's McpContext as the shared base (all brains share
    // the unified ~/.brain/brain.db so any instance's Db is the right one).
    let shared_ctx = brains
        .values()
        .next()
        .map(|inst| Arc::clone(&inst.mcp_context))
        .expect("at least one brain is initialised");
    let default_brain_id = shared_ctx.brain_id().to_string();
    let router = BrainRouter::new(shared_ctx, default_brain_id);

    let sock_path = brain_home()
        .map(|h| h.join("brain.sock"))
        .unwrap_or_else(|_| PathBuf::from("/tmp/brain.sock"));

    let (ipc_cancel, ipc_inode) = match IpcServer::bind(&sock_path, Arc::clone(&router)) {
        Ok(server) => {
            let token = server.cancellation_token();
            let inode = {
                use std::os::unix::fs::MetadataExt;
                std::fs::metadata(&sock_path).map(|m| m.ino()).unwrap_or(0)
            };
            tokio::spawn(async move { server.run().await });
            info!(path = ?sock_path, "IPC server started");
            (Some(token), inode)
        }
        Err(e) => {
            warn!(error = %e, "failed to start IPC server; continuing without IPC");
            (None, 0)
        }
    };

    // Startup self-heal: reset embedded_at if LanceDB is missing.
    for instance in brains.values() {
        embed_poll::self_heal_if_lance_missing(instance.pipeline.db(), instance.pipeline.store())
            .await;
    }

    // ── 4. Build path-to-brain lookup (longest prefix first) ────────────
    let mut prefix_map = build_prefix_map(&brains);

    // ── 5. Set up single BrainWatcher ────────────────────────────────────
    let (tx, mut rx) = tokio::sync::mpsc::channel(256);
    let mut watcher = BrainWatcher::new_empty(tx)?;

    for instance in brains.values() {
        for dir in &instance.note_dirs {
            if let Err(e) = watcher.watch_path(dir) {
                warn!(brain = %instance.name, dir = %dir.display(), error = %e, "failed to watch directory");
            }
        }
    }

    // ── 5b. Separate notify watcher for state_projection.toml ─────────────
    // BrainWatcher filters for .md files only (is_markdown()), so projection
    // events would be silently dropped. A separate watcher is required.
    let projection_path = brain_home()?.join(brain_lib::config::PROJECTION_FILENAME);
    let projection_dir = projection_path.parent().unwrap().to_path_buf();
    let (config_tx, mut config_rx) = tokio::sync::mpsc::channel::<()>(4);

    let _config_watcher = {
        let config_tx = config_tx.clone();
        let projection_file = projection_path.file_name().unwrap().to_owned();
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
    };

    // ── 6. Signal handlers ───────────────────────────────────────────────
    let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate())
        .expect("failed to register SIGTERM handler");

    let mut sighup = tokio::signal::unix::signal(SignalKind::hangup())
        .expect("failed to register SIGHUP handler");

    let mut sigusr1 = tokio::signal::unix::signal(SignalKind::user_defined1())
        .expect("failed to register SIGUSR1 handler");

    // Periodic optimization tick (same as single-brain run())
    let mut optimize_tick = tokio::time::interval(Duration::from_secs(60));

    // Job poll: reconcile recurring jobs, process ready jobs, GC old ones.
    let mut summarize_poll_interval = tokio::time::interval(Duration::from_secs(30));

    // In-memory lock set shared across all brains — prevents the reaper from
    // resetting jobs that are still actively running in a tokio::spawn task.
    let active_jobs = job_worker::ActiveJobs::new(job_worker::DEFAULT_MAX_CONCURRENT_JOBS);

    // Root validation tick: checks that registered roots still exist on disk,
    // prunes stale roots, and archives brains whose all roots are gone.
    let mut root_validation_tick = tokio::time::interval(Duration::from_secs(60));

    // ── 7. Event loop ─────────────────────────────────────────────────────
    let shutdown_reason = loop {
        tokio::select! {
            event = rx.recv() => {
                match event {
                    Some(first) => {
                        // Collect first + any immediately ready events (50ms window)
                        let mut batch = vec![first];
                        let deadline = tokio::time::Instant::now() + Duration::from_millis(50);
                        while let Ok(Some(evt)) = tokio::time::timeout_at(deadline, rx.recv()).await {
                            batch.push(evt);
                        }

                        // Route each event to the correct brain's work queue
                        for evt in batch {
                            let event_path = event_primary_path(&evt);
                            if let Some(brain_name) = lookup_brain(&prefix_map, &event_path) {
                                if let Some(instance) = brains.get_mut(&brain_name) {
                                    instance.work_queue.push(evt);
                                }
                            } else {
                                debug!(path = %event_path.display(), "event path matches no brain, dropping");
                            }
                        }

                        // Process each brain's work queue
                        for instance in brains.values_mut() {
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
                for instance in brains.values() {
                    instance.pipeline.store().optimizer().maybe_optimize().await;
                }
            }
            _ = summarize_poll_interval.tick() => {
                for instance in brains.values() {
                    // Reconcile recurring singleton jobs (idempotent).
                    let brain_infos = vec![recurring_jobs::BrainInfo {
                        brain_id: instance.mcp_context.brain_id().to_string(),
                    }];
                    if let Err(e) = recurring_jobs::reconcile_recurring_jobs(instance.pipeline.db(), &brain_infos) {
                        tracing::warn!(brain = %instance.name, error = %e, "reconcile_recurring_jobs failed");
                    }

                    if let Err(e) = job_worker::reap_stuck_jobs_filtered(instance.pipeline.db(), &active_jobs) {
                        tracing::warn!(brain = %instance.name, error = %e, "reap_stuck_jobs failed");
                    }
                    let n = job_worker::process_jobs(
                        instance.pipeline.db(),
                        instance.pipeline.store(),
                        instance.pipeline.embedder(),
                        &active_jobs,
                        20,
                    ).await;
                    if n > 0 {
                        info!(brain = %instance.name, processed = n, "jobs dispatched");
                    }
                    let protected = recurring_jobs::protected_kinds();
                    if let Err(e) = instance.pipeline.db().gc_completed_jobs(7 * 86400, &protected) {
                        tracing::warn!(brain = %instance.name, error = %e, "gc_completed_jobs failed");
                    }
                }
            }
            _ = root_validation_tick.tick() => {
                if validate_roots(&mut brains, &mut watcher, &shared_db) {
                    prefix_map = build_prefix_map(&brains);
                }
            }
            _ = config_rx.recv() => {
                // Config.toml is a projection of DB state. If it was edited
                // manually, overwrite it with DB state. If we wrote it ourselves
                // (via project_db_to_config), this is a harmless no-op.
                info!("state_projection.toml changed, re-projecting from DB");
                project_db_to_config(&shared_db);
            }
            _ = sighup.recv() => {
                info!("received SIGHUP, reloading brain registry");
                match reload_and_project(&mut brains, &mut watcher, Arc::clone(&embedder), &shared_db, &shared_store).await {
                    Ok(()) => {
                        prefix_map = build_prefix_map(&brains);
                        info!(brains = brains.len(), "brain registry reloaded");
                    }
                    Err(e) => {
                        warn!(error = %e, "brain registry reload failed, continuing with existing config");
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
                // Log a basic status snapshot to stderr
                let total_brains = brains.len();
                eprintln!("multi-brain daemon: {} brain(s) active", total_brains);
            }
        }
    };

    // ── 8. Shutdown sequence ──────────────────────────────────────────────

    // Phase 1: Stop IPC server and remove socket file FIRST.
    // This must happen before the long drain/WAL/LanceDB phases to prevent
    // a race where a new daemon binds a fresh socket and then our late
    // cleanup deletes it (the bug that caused "Broken pipe" errors).
    info!("shutdown phase 1/5: stopping IPC server");
    if let Some(token) = ipc_cancel {
        token.cancel();
    }
    // Only delete the socket if it's still ours (same inode as at bind time).
    // A new daemon may have already replaced it during a stale-binary restart.
    if ipc_inode != 0 {
        brain_lib::ipc::server::remove_socket_if_owned(&sock_path, ipc_inode);
    }

    // Phase 2: Stop watcher
    info!("shutdown phase 2/5: stopping file watcher");
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
            if let Err(e) = instance.pipeline.db().wal_checkpoint() {
                warn!(brain = %instance.name, error = %e, "WAL checkpoint failed");
            }
        }

        // Phase 5: LanceDB optimize (single shared store)
        info!("shutdown phase 5/5: optimizing LanceDB");
        shared_store.optimizer().force_optimize().await;
    } else {
        info!("shutdown phases 4-5: skipped (force shutdown)");
    }

    // Phase 5: Done (IPC was already stopped in phase 1).
    let clean = !force_shutdown && total_dropped == 0;
    info!(clean, dropped_items = total_dropped, "shutdown complete");

    Ok(ShutdownOutcome {
        clean,
        dropped_items: total_dropped,
    })
}

/// Initialise a single [`BrainInstance`] from a brain name and note directories.
///
/// Accepts shared `Db` and `Store` handles — all brains use the unified
/// `~/.brain/brain.db` and `~/.brain/lancedb/`.
async fn init_brain_instance(
    name: &str,
    notes: Vec<PathBuf>,
    embedder: Arc<dyn Embed>,
    brain_id: &str,
    db: Db,
    store: Store,
) -> Result<BrainInstance> {
    let paths = resolve_paths_for_brain(name)?;

    // Validate that at least one note directory exists
    let note_dirs: Vec<PathBuf> = notes
        .into_iter()
        .filter(|p| {
            if p.exists() {
                true
            } else {
                warn!(brain = %name, dir = %p.display(), "note directory does not exist, skipping");
                false
            }
        })
        .collect();

    // Create the pipeline with the shared embedder and shared store.
    // Schema version check is already done in run_multi() before cloning.
    let mut pipeline = IndexPipeline::with_embedder(db, store, embedder).await?;
    pipeline.set_brain_id(brain_id.to_string());

    // Resolve LLM provider: env vars first, then DB-backed credentials.
    let brain_home =
        brain_lib::config::brain_home().unwrap_or_else(|_| std::path::PathBuf::from("."));
    if let Some(provider) = brain_lib::llm::resolve_provider_with_db(pipeline.db(), &brain_home) {
        pipeline.set_summarizer(Arc::from(provider));
    }

    // Build MCP context from the pipeline's stores + task/record/object stores.
    // Derive brain_data_dir from the per-brain data directory.
    let brain_data_dir = paths
        .sqlite_db
        .parent()
        .map(|h| h.join("brains").join(name))
        .unwrap_or_else(|| std::path::PathBuf::from("."));

    let brain_home_path = brain_data_dir
        .parent() // brains/
        .and_then(|p| p.parent()) // $BRAIN_HOME
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::path::PathBuf::from("."));

    let stores = brain_lib::stores::BrainStores::from_dbs(
        pipeline.db().clone(),
        brain_id,
        &brain_data_dir,
        &brain_home_path,
    )?;
    let metrics = Arc::clone(pipeline.metrics());

    let mcp_context = McpContext::from_stores(
        stores, None, // search: no embedder available in daemon context
        None, // writable_store: pipeline.store() is owned by pipeline; IPC is read-only
        metrics,
    );

    Ok(BrainInstance {
        name: name.to_string(),
        pipeline,
        work_queue: WorkQueue::default(),
        note_dirs,
        mcp_context,
    })
}

/// Build a sorted prefix lookup: `Vec<(prefix_path, brain_name)>`, longest
/// prefix first so that more-specific paths match before shorter ones.
fn build_prefix_map(brains: &HashMap<String, BrainInstance>) -> Vec<(PathBuf, String)> {
    let mut map: Vec<(PathBuf, String)> = brains
        .values()
        .flat_map(|inst| {
            inst.note_dirs
                .iter()
                .map(|dir| (dir.clone(), inst.name.clone()))
                .collect::<Vec<_>>()
        })
        .collect();

    // Sort by path component count descending (longest prefix first)
    map.sort_by(|(a, _), (b, _)| b.components().count().cmp(&a.components().count()));

    map
}

/// Given an event path, find the brain whose note directory is the longest
/// prefix of that path.
fn lookup_brain(prefix_map: &[(PathBuf, String)], event_path: &Path) -> Option<String> {
    for (prefix, brain_name) in prefix_map {
        if event_path.starts_with(prefix) {
            return Some(brain_name.clone());
        }
    }
    None
}

/// Extract the primary path from a [`FileEvent`] for routing purposes.
fn event_primary_path(event: &FileEvent) -> PathBuf {
    match event {
        FileEvent::Changed(p) | FileEvent::Created(p) | FileEvent::Deleted(p) => p.clone(),
        FileEvent::Renamed { to, .. } => to.clone(),
    }
}

/// Ensure every registered brain has a stable ID in both its local
/// `brain.toml` and the global registry. Generates missing IDs on the fly.
fn sync_brain_ids(global_cfg: &brain_lib::config::GlobalConfig) {
    for (name, entry) in &global_cfg.brains {
        let Some(root) = entry.primary_root() else {
            continue;
        };
        let brain_dir = root.join(".brain");
        match get_or_generate_brain_id(&brain_dir) {
            Ok(id) => {
                if entry.id.as_deref() != Some(&id) {
                    info!(brain = %name, id = %id, "synced brain ID to registry");
                }
            }
            Err(e) => {
                warn!(brain = %name, error = %e, "failed to sync brain ID");
            }
        }
    }
}

/// Reload the brain registry and re-project all brains into the SQL brains table.
///
/// Combines `reload_brains()` with a fresh projection so that SQL-based brain
/// resolution stays in sync after any config change or SIGHUP. Called by both
/// the state_projection.toml watcher and the SIGHUP handler.
async fn reload_and_project(
    brains: &mut HashMap<String, BrainInstance>,
    watcher: &mut BrainWatcher,
    embedder: Arc<dyn Embed>,
    shared_db: &Db,
    shared_store: &Store,
) -> Result<()> {
    reload_brains(
        brains,
        watcher,
        Arc::clone(&embedder),
        shared_db,
        shared_store,
    )
    .await?;

    // Load the freshest config after reload to build projections.
    let cfg = load_global_config()?;

    let db = brains
        .values()
        .next()
        .map(|inst| inst.mcp_context.stores.db())
        .unwrap_or(shared_db);

    // Read existing prefixes from DB to preserve manual overrides.
    let existing_prefixes: std::collections::HashMap<String, String> = db
        .list_brains(false)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|row| row.prefix.map(|p| (row.brain_id, p)))
        .collect();

    let projections: Vec<BrainProjection> = brains
        .iter()
        .filter_map(|(name, inst)| {
            let bid = inst.mcp_context.brain_id().to_string();
            cfg.brains.get(name).map(|entry| {
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

    if let Err(e) = db.project_config_to_brains(&projections) {
        warn!(error = %e, "failed to re-sync brains into DB");
    }

    // Sync prefixes from DB back to state_projection.toml.
    sync_prefixes_to_config(db, brains);

    Ok(())
}

/// Reload the brain registry from disk, diffing against the current state.
///
/// - New brains: initialise and watch
/// - Removed brains: unwatch and drop
/// - Updated brains (notes dirs changed): unwatch old, watch new
async fn reload_brains(
    brains: &mut HashMap<String, BrainInstance>,
    watcher: &mut BrainWatcher,
    embedder: Arc<dyn Embed>,
    shared_db: &Db,
    shared_store: &Store,
) -> Result<()> {
    let new_cfg = load_global_config()?;

    // Collect owned name sets before mutating `brains`
    let new_names: std::collections::HashSet<String> = new_cfg.brains.keys().cloned().collect();
    let old_names: std::collections::HashSet<String> = brains.keys().cloned().collect();

    // Removed brains: unwatch dirs and remove from map
    let removed: Vec<String> = old_names.difference(&new_names).cloned().collect();

    for name in &removed {
        if let Some(instance) = brains.remove(name) {
            for dir in &instance.note_dirs {
                if let Err(e) = watcher.unwatch_path(dir) {
                    warn!(brain = %name, dir = %dir.display(), error = %e, "failed to unwatch directory");
                }
            }
            info!(brain = %name, "brain removed from registry");
        }
    }

    // New brains: initialise and watch
    let added: Vec<String> = new_names.difference(&old_names).cloned().collect();

    for name in &added {
        if let Some(entry) = new_cfg.brains.get(name) {
            let brain_id = match resolve_brain_id(entry, name) {
                Ok(id) => id,
                Err(e) => {
                    warn!(brain = %name, error = %e, "failed to resolve brain ID for new brain, skipping");
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
                    for dir in &instance.note_dirs {
                        if let Err(e) = watcher.watch_path(dir) {
                            warn!(brain = %name, dir = %dir.display(), error = %e, "failed to watch new brain directory");
                        }
                    }
                    info!(brain = %name, "new brain added to registry");
                    brains.insert(name.clone(), instance);
                }
                Err(e) => {
                    warn!(brain = %name, error = %e, "failed to initialise new brain, skipping");
                }
            }
        }
    }

    // Updated brains: check if note dirs changed
    let updated: Vec<String> = old_names.intersection(&new_names).cloned().collect();
    for name in updated {
        let new_entry = match new_cfg.brains.get(&name) {
            Some(e) => e,
            None => continue,
        };
        let instance = match brains.get_mut(&name) {
            Some(i) => i,
            None => continue,
        };

        let old_dirs: std::collections::HashSet<&PathBuf> = instance.note_dirs.iter().collect();
        let new_dirs_raw: std::collections::HashSet<&PathBuf> = new_entry.notes.iter().collect();

        if old_dirs == new_dirs_raw {
            continue; // No change
        }

        // Unwatch removed dirs
        for dir in old_dirs.difference(&new_dirs_raw) {
            if let Err(e) = watcher.unwatch_path(dir) {
                warn!(brain = %name, dir = %dir.display(), error = %e, "failed to unwatch old directory");
            }
        }

        // Keep unchanged dirs, watch only truly new ones
        let unchanged: Vec<PathBuf> = old_dirs
            .intersection(&new_dirs_raw)
            .map(|d| (*d).clone())
            .collect();
        let mut new_note_dirs = unchanged;

        for dir in new_dirs_raw.difference(&old_dirs) {
            if dir.exists() {
                if let Err(e) = watcher.watch_path(dir) {
                    warn!(brain = %name, dir = %dir.display(), error = %e, "failed to watch new directory");
                } else {
                    new_note_dirs.push((*dir).clone());
                }
            } else {
                warn!(brain = %name, dir = %dir.display(), "new note directory does not exist, skipping");
            }
        }

        instance.note_dirs = new_note_dirs;
        info!(brain = %name, "brain note directories updated");
    }

    Ok(())
}

/// Validate that all registered brain roots still exist on disk.
///
/// For each brain in the active `brains` map:
/// - Loads the current global config from disk.
/// - Removes roots that no longer exist from the config entry.
/// - If a brain has no roots remaining, marks it archived in config and DB,
///   unwatches all its note directories, and removes it from the map.
/// - If a brain retains some roots, unwatches note dirs that fall under
///   removed roots and updates `instance.note_dirs`.
/// - Projects the updated DB state to state_projection.toml.
///
/// The DB is the source of truth. Config.toml is a projection.
///
/// Returns `true` if the prefix map needs rebuilding (any brain changed).
fn validate_roots(
    brains: &mut HashMap<String, BrainInstance>,
    watcher: &mut BrainWatcher,
    db: &brain_persistence::db::Db,
) -> bool {
    // Read active brains from DB (source of truth).
    let db_brains = match db.list_brains(true) {
        Ok(rows) => rows,
        Err(e) => {
            warn!(error = %e, "root validation: failed to read brains from DB, skipping");
            return false;
        }
    };

    let mut db_changed = false;
    let mut prefix_map_dirty = false;

    for row in &db_brains {
        let roots: Vec<std::path::PathBuf> = row
            .roots_json
            .as_deref()
            .and_then(|json| serde_json::from_str::<Vec<String>>(json).ok())
            .unwrap_or_default()
            .into_iter()
            .map(std::path::PathBuf::from)
            .collect();

        let stale_roots: Vec<&std::path::PathBuf> = roots.iter().filter(|r| !r.exists()).collect();

        if stale_roots.is_empty() {
            continue;
        }

        for root in &stale_roots {
            info!(brain = %row.name, root = %root.display(), "removing stale root (DB source of truth)");
        }

        let remaining_roots: Vec<&std::path::PathBuf> =
            roots.iter().filter(|r| !stale_roots.contains(r)).collect();

        if remaining_roots.is_empty() {
            // All roots gone — atomically archive + clear roots in DB.
            info!(brain = %row.name, "all roots gone; archiving brain in DB");
            if let Err(e) = db.archive_and_clear_roots(&row.brain_id) {
                warn!(brain = %row.name, error = %e, "failed to archive brain in DB");
                continue;
            }
            db_changed = true;

            // Remove from in-memory map and unwatch.
            if let Some(instance) = brains.remove(&row.name) {
                for dir in &instance.note_dirs {
                    if let Err(e) = watcher.unwatch_path(dir) {
                        warn!(brain = %row.name, dir = %dir.display(), error = %e, "failed to unwatch dir during archival");
                    }
                }
            }
            prefix_map_dirty = true;
        } else {
            // Some roots remain — update DB with surviving roots.
            let remaining_strs: Vec<&str> =
                remaining_roots.iter().filter_map(|r| r.to_str()).collect();
            let new_roots_json =
                serde_json::to_string(&remaining_strs).unwrap_or_else(|_| "[]".to_string());

            if let Err(e) = db.update_brain_roots(&row.brain_id, &new_roots_json) {
                warn!(brain = %row.name, error = %e, "failed to update roots in DB");
                continue;
            }
            db_changed = true;

            // Prune note dirs under stale roots from in-memory map.
            if let Some(instance) = brains.get_mut(&row.name) {
                let removed_note_dirs: Vec<std::path::PathBuf> = instance
                    .note_dirs
                    .iter()
                    .filter(|dir| stale_roots.iter().any(|root| dir.starts_with(root)))
                    .cloned()
                    .collect();

                for dir in &removed_note_dirs {
                    info!(brain = %row.name, dir = %dir.display(), "unwatching note dir under stale root");
                    if let Err(e) = watcher.unwatch_path(dir) {
                        warn!(brain = %row.name, dir = %dir.display(), error = %e, "failed to unwatch dir");
                    }
                }

                if !removed_note_dirs.is_empty() {
                    instance
                        .note_dirs
                        .retain(|d| !removed_note_dirs.contains(d));
                    prefix_map_dirty = true;
                }
            }
        }
    }

    // Project DB state → state_projection.toml (config is a read-only projection).
    if db_changed {
        project_db_to_config(db);
    }

    prefix_map_dirty
}

/// Project DB brain state to state_projection.toml.
///
/// Reads all brains from the DB and overwrites state_projection.toml roots, notes,
/// aliases, and archived status. Config.toml is a projection, not a source.
fn project_db_to_config(db: &brain_persistence::db::Db) {
    let mut cfg = match load_global_config() {
        Ok(cfg) => cfg,
        Err(e) => {
            warn!(error = %e, "project_db_to_config: failed to load config");
            return;
        }
    };

    let db_brains = match db.list_brains(false) {
        Ok(rows) => rows,
        Err(e) => {
            warn!(error = %e, "project_db_to_config: failed to read brains from DB");
            return;
        }
    };

    let mut changed = false;
    for row in &db_brains {
        let entry = match cfg.brains.get_mut(&row.name) {
            Some(e) => e,
            None => continue,
        };

        // Project roots from DB.
        let db_roots: Vec<std::path::PathBuf> = row
            .roots_json
            .as_deref()
            .and_then(|json| serde_json::from_str::<Vec<String>>(json).ok())
            .unwrap_or_default()
            .into_iter()
            .map(std::path::PathBuf::from)
            .collect();

        if entry.roots != db_roots {
            entry.roots = db_roots;
            changed = true;
        }

        if entry.archived != row.archived {
            entry.archived = row.archived;
            changed = true;
        }
    }

    if changed {
        cfg.last_projected_at = Some(chrono::Utc::now().to_rfc3339());
        if let Err(e) = save_global_config(&cfg) {
            warn!(error = %e, "project_db_to_config: failed to save config");
        }
    }
}

/// Sync prefixes from the DB (source of truth) back to state_projection.toml (projection).
///
/// Only writes if any prefix actually changed, to avoid triggering the config
/// watcher unnecessarily.
fn sync_prefixes_to_config(
    db: &brain_persistence::db::Db,
    brains: &HashMap<String, BrainInstance>,
) {
    let Ok(mut cfg) = load_global_config() else {
        return;
    };
    let mut changed = false;
    for (name, inst) in brains {
        let brain_id = inst.mcp_context.brain_id().to_string();
        if let Ok(Some(db_prefix)) = db.get_brain_prefix(&brain_id)
            && let Some(entry) = cfg.brains.get_mut(name)
            && entry.prefix.as_deref() != Some(&db_prefix)
        {
            entry.prefix = Some(db_prefix);
            changed = true;
        }
    }
    if changed {
        if let Err(e) = save_global_config(&cfg) {
            warn!(error = %e, "failed to sync prefix to state_projection.toml");
        } else {
            debug!("synced prefixes from DB to state_projection.toml");
        }
    }
}
