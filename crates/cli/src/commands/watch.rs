use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use anyhow::{Result, bail};
use brain_lib::config::paths::normalize_note_paths_lenient;
use brain_lib::config::{
    brain_home, get_or_generate_brain_id, load_global_config, resolve_brain_id,
    resolve_paths_for_brain,
};
use brain_lib::db::Db;
use brain_lib::embedder::Embedder;
use brain_lib::ipc::router::BrainRouter;
use brain_lib::ipc::server::IpcServer;
use brain_lib::mcp::McpContext;
use brain_lib::pipeline::IndexPipeline;
use brain_lib::pipeline::consolidation::ConsolidationScheduler;
use brain_lib::pipeline::embed_poll;
use brain_lib::prelude::*;
use brain_lib::store::{Store, StoreReader};
use brain_lib::summarizer::FlanT5Summarizer;
use tracing::{debug, info, warn};

// The daemon (daemon.rs) uses libc and sends SIGTERM — unix-only.
use tokio::signal::unix::SignalKind;

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

    // Try to load the summarizer if the model directory exists alongside the embedder
    let summarizer_dir = model_dir.parent().map(|p| p.join("flan-t5-small"));
    if let Some(ref dir) = summarizer_dir
        && dir.is_dir()
    {
        match FlanT5Summarizer::load(dir) {
            Ok(s) => {
                info!(model_dir = %dir.display(), "loaded Flan-T5 summarizer");
                pipeline.set_summarizer(Arc::new(s));
            }
            Err(e) => {
                warn!(error = %e, "failed to load summarizer, consolidation disabled");
            }
        }
    }

    // Full scan on startup to catch offline changes
    let stats = pipeline.full_scan(&note_dirs).await?;
    info!(
        indexed = stats.indexed,
        skipped = stats.skipped,
        deleted = stats.deleted,
        "startup scan complete"
    );

    // Compact fragments created during full scan
    pipeline.store().optimizer().force_optimize().await;

    // Startup self-heal: if LanceDB is missing, reset embedded_at so all
    // tasks and chunks will be re-embedded on the next poll cycle.
    embed_poll::self_heal_if_lance_missing(pipeline.db(), pipeline.db(), pipeline.store()).await;

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

    // Consolidation scheduler: runs ML summarization when the system has been
    // idle for 5 minutes. The tick is cheap (atomic load); actual work only
    // happens when a summarizer is configured and idle threshold is met.
    let last_event_ts = Arc::new(AtomicU64::new(0));
    let consolidator = ConsolidationScheduler::new(last_event_ts.clone());
    let mut consolidation_tick = tokio::time::interval(Duration::from_secs(30));

    // Embedding poll: catches tasks/chunks that missed inline embedding
    // (e.g. written directly to SQLite, or failed during MCP handler).
    let mut embed_poll_interval = tokio::time::interval(Duration::from_secs(10));

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

                        // 7. Stamp the last-event timestamp for idle detection
                        consolidator.record_file_event();
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
            _ = consolidation_tick.tick() => {
                if let Some(summarizer) = pipeline.summarizer()
                    && let Err(e) = consolidator.maybe_consolidate(pipeline.db(), summarizer).await
                {
                    tracing::warn!("consolidation error: {e}");
                }
            }
            _ = embed_poll_interval.tick() => {
                let n_tasks = embed_poll::poll_stale_tasks(
                    pipeline.db(),
                    pipeline.store(),
                    pipeline.embedder(),
                    "",
                ).await;
                let n_chunks = embed_poll::poll_stale_chunks(
                    pipeline.db(),
                    pipeline.store(),
                    pipeline.embedder(),
                ).await;
                if n_tasks > 0 || n_chunks > 0 {
                    debug!(tasks = n_tasks, chunks = n_chunks, "embed_poll cycle complete");
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
                    .with_read_conn(brain_lib::db::files::find_stuck_files)
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
    consolidator: ConsolidationScheduler,
    mcp_context: Arc<McpContext>,
}

/// Watch all registered brains from the global config for changes and
/// re-index incrementally.
///
/// Reads `~/.brain/config.toml`, creates a separate [`IndexPipeline`] for
/// each registered brain (sharing a single embedder), and routes file events
/// to the correct pipeline via longest-prefix matching.  Handles SIGHUP to
/// reload the brain registry without restarting.
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

    // ── 2. Load embedder once (model_dir is the same for all brains) ─────
    // Use the first registered brain to derive model_dir.
    let first_name = global_cfg.brains.keys().next().expect("non-empty map");
    let first_paths = resolve_paths_for_brain(first_name)?;
    let model_dir = first_paths.model_dir.clone();

    let embedder: Arc<dyn Embed> = {
        let model_dir_clone = model_dir.clone();
        let loaded = tokio::task::spawn_blocking(move || Embedder::load(&model_dir_clone))
            .await
            .map_err(|e| anyhow::anyhow!("spawn_blocking embedder: {e}"))??;
        Arc::new(loaded)
    };

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
        match init_brain_instance(name, entry.notes.clone(), Arc::clone(&embedder), &brain_id).await
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

    // ── 3b. Start IPC server ─────────────────────────────────────────────
    // Build brain name → brain_id map for the router.
    let ipc_brain_map: HashMap<String, String> = brains
        .iter()
        .map(|(name, inst)| (name.clone(), inst.mcp_context.brain_id.clone()))
        .collect();
    // Use the first brain's McpContext as the shared base (all brains share
    // the unified ~/.brain/brain.db so any instance's Db is the right one).
    let shared_ctx = brains
        .values()
        .next()
        .map(|inst| Arc::clone(&inst.mcp_context))
        .expect("at least one brain is initialised");
    let router = BrainRouter::new(shared_ctx, ipc_brain_map);

    let sock_path = brain_home()
        .map(|h| h.join("brain.sock"))
        .unwrap_or_else(|_| PathBuf::from("/tmp/brain.sock"));

    let ipc_cancel = match IpcServer::bind(&sock_path, Arc::clone(&router)) {
        Ok(server) => {
            let token = server.cancellation_token();
            tokio::spawn(async move { server.run().await });
            info!(path = ?sock_path, "IPC server started");
            Some(token)
        }
        Err(e) => {
            warn!(error = %e, "failed to start IPC server; continuing without IPC");
            None
        }
    };

    // Startup self-heal: per-brain — reset embedded_at if LanceDB is missing.
    for instance in brains.values() {
        embed_poll::self_heal_if_lance_missing(
            instance.pipeline.db(),
            instance.pipeline.db(),
            instance.pipeline.store(),
        )
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

    // ── 6. Signal handlers ───────────────────────────────────────────────
    let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate())
        .expect("failed to register SIGTERM handler");

    let mut sighup = tokio::signal::unix::signal(SignalKind::hangup())
        .expect("failed to register SIGHUP handler");

    let mut sigusr1 = tokio::signal::unix::signal(SignalKind::user_defined1())
        .expect("failed to register SIGUSR1 handler");

    // Periodic optimization tick (same as single-brain run())
    let mut optimize_tick = tokio::time::interval(Duration::from_secs(60));

    // Consolidation tick: cheap idle check every 30s; actual work gated on
    // per-brain idle threshold (5min) and summarizer being configured.
    let mut consolidation_tick = tokio::time::interval(Duration::from_secs(30));

    // Embedding poll: catches tasks/chunks that missed inline embedding.
    let mut embed_poll_interval = tokio::time::interval(Duration::from_secs(10));

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
                                    instance.consolidator.record_file_event();
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
            _ = consolidation_tick.tick() => {
                for instance in brains.values() {
                    if let Some(summarizer) = instance.pipeline.summarizer()
                        && let Err(e) = instance.consolidator.maybe_consolidate(
                            instance.pipeline.db(),
                            summarizer,
                        ).await
                    {
                        tracing::warn!(brain = %instance.name, "consolidation error: {e}");
                    }
                }
            }
            _ = embed_poll_interval.tick() => {
                for instance in brains.values() {
                    let brain_id = &instance.mcp_context.brain_id;
                    let n_tasks = embed_poll::poll_stale_tasks(
                        instance.pipeline.db(),
                        instance.pipeline.store(),
                        instance.pipeline.embedder(),
                        brain_id,
                    ).await;
                    let n_chunks = embed_poll::poll_stale_chunks(
                        instance.pipeline.db(),
                        instance.pipeline.store(),
                        instance.pipeline.embedder(),
                    ).await;
                    if n_tasks > 0 || n_chunks > 0 {
                        debug!(
                            brain = %instance.name,
                            tasks = n_tasks,
                            chunks = n_chunks,
                            "embed_poll cycle complete"
                        );
                    }
                }
            }
            _ = sighup.recv() => {
                info!("received SIGHUP, reloading brain registry");
                match reload_brains(&mut brains, &mut watcher, Arc::clone(&embedder)).await {
                    Ok(()) => {
                        prefix_map = build_prefix_map(&brains);
                        // Rebuild brain_id map and update the IPC router.
                        let updated_brain_map: HashMap<String, String> = brains
                            .iter()
                            .map(|(name, inst)| (name.clone(), inst.mcp_context.brain_id.clone()))
                            .collect();
                        router.update_brains(updated_brain_map).await;
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

    // Phase 1: Stop watcher
    info!("shutdown phase 1/5: stopping file watcher");
    drop(watcher);

    // Phase 2: Drain pending work queues (signal shutdown only)
    let mut total_dropped: usize = 0;
    let mut force_shutdown = false;

    if matches!(shutdown_reason, ShutdownReason::Signal) {
        info!("shutdown phase 2/5: draining pending work queues (10s timeout)");

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
        info!("shutdown phase 2/5: channel closed, skipping drain");
    }

    if !force_shutdown {
        // Phase 3: SQLite WAL checkpoint for all brains
        info!("shutdown phase 3/5: checkpointing SQLite WAL");
        for instance in brains.values() {
            if let Err(e) = instance.pipeline.db().wal_checkpoint() {
                warn!(brain = %instance.name, error = %e, "WAL checkpoint failed");
            }
        }

        // Phase 4: LanceDB optimize for all brains
        info!("shutdown phase 4/5: optimizing LanceDB");
        for instance in brains.values() {
            instance.pipeline.store().optimizer().force_optimize().await;
        }
    } else {
        info!("shutdown phases 3-5: skipped (force shutdown)");
    }

    // Phase 5: Stop IPC server and remove socket file
    info!("shutdown phase 5/5: stopping IPC server");
    if let Some(token) = ipc_cancel {
        token.cancel();
    }
    let _ = std::fs::remove_file(&sock_path);

    let clean = !force_shutdown && total_dropped == 0;
    info!(clean, dropped_items = total_dropped, "shutdown complete");

    Ok(ShutdownOutcome {
        clean,
        dropped_items: total_dropped,
    })
}

/// Initialise a single [`BrainInstance`] from a brain name and note directories.
async fn init_brain_instance(
    name: &str,
    notes: Vec<PathBuf>,
    embedder: Arc<dyn Embed>,
    brain_id: &str,
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

    // Open the SQLite database (sync, needs spawn_blocking)
    let sqlite_path = paths.sqlite_db.clone();
    let db = tokio::task::spawn_blocking(move || Db::open(&sqlite_path))
        .await
        .map_err(|e| anyhow::anyhow!("spawn_blocking Db::open: {e}"))??;

    // Open or create the LanceDB store
    let store = Store::open_or_create(&paths.lance_db).await?;

    // Create the pipeline with the shared embedder
    let mut pipeline = IndexPipeline::with_embedder(db, store, embedder).await?;

    // Load summarizer if model is available
    if let Some(ref dir) = paths.summarizer_model_dir {
        match FlanT5Summarizer::load(dir) {
            Ok(s) => {
                info!(brain = %name, model_dir = %dir.display(), "loaded Flan-T5 summarizer");
                pipeline.set_summarizer(Arc::new(s));
            }
            Err(e) => {
                warn!(brain = %name, error = %e, "failed to load summarizer, consolidation disabled");
            }
        }
    }

    // Run initial full scan with self-healing on failure
    if !note_dirs.is_empty() {
        match pipeline.full_scan(&note_dirs).await {
            Ok(stats) => {
                info!(
                    brain = %name,
                    indexed = stats.indexed,
                    skipped = stats.skipped,
                    deleted = stats.deleted,
                    "startup scan complete"
                );
            }
            Err(e) => {
                warn!(brain = %name, error = %e, "startup scan failed, attempting repair");
                match pipeline.repair().await {
                    Ok(()) => {
                        info!(brain = %name, "repair complete, retrying scan");
                        match pipeline.full_scan(&note_dirs).await {
                            Ok(stats) => {
                                info!(
                                    brain = %name,
                                    indexed = stats.indexed,
                                    skipped = stats.skipped,
                                    deleted = stats.deleted,
                                    "post-repair scan complete"
                                );
                            }
                            Err(e2) => {
                                warn!(brain = %name, error = %e2, "scan failed even after repair");
                            }
                        }
                    }
                    Err(re) => {
                        warn!(brain = %name, error = %re, "repair failed");
                    }
                }
            }
        }
    }

    let last_event_ts = Arc::new(AtomicU64::new(0));
    let consolidator = ConsolidationScheduler::new(last_event_ts);

    // Build MCP context from the pipeline's stores + task/record/object stores.
    let brain_data_dir = paths
        .sqlite_db
        .parent()
        .unwrap_or(std::path::Path::new("."));

    let brain_home_path = brain_data_dir
        .parent() // brains/
        .and_then(|p| p.parent()) // $BRAIN_HOME
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::path::PathBuf::from("."));

    let stores = brain_lib::stores::BrainStores::from_dbs(
        pipeline.db().clone(),
        brain_id,
        brain_data_dir,
        &brain_home_path,
    )?;
    let store_reader = StoreReader::from_store(pipeline.store());
    let metrics = Arc::clone(pipeline.metrics());

    let mcp_context = McpContext::from_stores(
        stores.db().clone(),
        Some(store_reader),
        None, // writable_store: pipeline.store() is owned by pipeline; IPC is read-only
        None, // embedder: not needed for task/record operations via IPC
        stores.tasks,
        stores.records,
        stores.objects,
        metrics,
        stores.brain_home,
        stores.brain_name,
    );

    Ok(BrainInstance {
        name: name.to_string(),
        pipeline,
        work_queue: WorkQueue::default(),
        note_dirs,
        consolidator,
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
        let brain_dir = entry.primary_root().join(".brain");
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

/// Reload the brain registry from disk, diffing against the current state.
///
/// - New brains: initialise and watch
/// - Removed brains: unwatch and drop
/// - Updated brains (notes dirs changed): unwatch old, watch new
async fn reload_brains(
    brains: &mut HashMap<String, BrainInstance>,
    watcher: &mut BrainWatcher,
    embedder: Arc<dyn Embed>,
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
            match init_brain_instance(name, entry.notes.clone(), Arc::clone(&embedder), &brain_id)
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
