use std::path::PathBuf;

use anyhow::{Context, Result};
use tracing_subscriber::EnvFilter;

use crate::cli::*;
use crate::commands;
use crate::hooks::OutputFormat;

// ── helpers ─────────────────────────────────────────────────

/// Try to find the brain project root by walking up from cwd.
/// Returns `Ok(Some(root))` if found, `Ok(None)` if no marker file exists.
fn resolve_brain_root() -> Result<Option<PathBuf>> {
    let cwd = std::env::current_dir()?;
    Ok(brain_lib::config::find_brain_root(&cwd))
}

/// Initialize tracing for CLI (interactive) usage.
///
/// Writes to stderr. Respects the `BRAIN_LOG_FORMAT=json` env var.
pub(crate) fn init_tracing_for_cli() -> anyhow::Result<()> {
    let env_filter = EnvFilter::from_default_env().add_directive("info".parse()?);
    let use_json = std::env::var("BRAIN_LOG_FORMAT")
        .map(|v| v.eq_ignore_ascii_case("json"))
        .unwrap_or(false);

    if use_json {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_writer(std::io::stderr)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_writer(std::io::stderr)
            .init();
    }
    Ok(())
}

// ── async dispatch ──────────────────────────────────────────

pub(crate) async fn async_main(cli: Cli) -> Result<()> {
    // Daemon path: tracing is initialized post-fork inside daemon.rs.
    // All other commands initialize here for CLI usage.
    let is_daemon_start = matches!(
        &cli.command,
        Command::Daemon {
            action: DaemonAction::Start { .. },
        }
    );
    if !is_daemon_start {
        init_tracing_for_cli()?;
    }

    // Warn if ~/.brain has overly broad permissions.
    let _ = brain_lib::config::check_brain_home_permissions();

    match cli.command {
        #[cfg(feature = "embed")]
        Command::Index { notes_path } => {
            commands::index::run(notes_path, cli.model_dir, cli.lance_db, cli.sqlite_db).await?
        }
        #[cfg(feature = "embed")]
        Command::Watch { notes_path } => commands::watch::run(notes_path)?,
        Command::Daemon { action } => {
            let daemon = commands::daemon::Daemon::new()?;
            match action {
                #[cfg(feature = "embed")]
                DaemonAction::Start { .. } => {
                    // Child process after fork — run the multi-brain supervisor.
                    // Log init already done in daemon.rs::start() post-fork.
                    let (control_tx, control_rx) = tokio::sync::mpsc::channel(64);
                    // No co-located RPC server on this path — pass a detached
                    // shutdown handle so the supervisor's Phase 1 still
                    // type-checks but has nothing to flip.
                    let outcome = brain_daemon::watcher::Supervisor::bootstrap_and_run(
                        control_rx,
                        brain_daemon::ShutdownHandle::noop(),
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!("supervisor: {e}"))?;
                    drop(control_tx);
                    if !outcome.clean {
                        std::process::exit(1);
                    }
                }
                #[cfg(not(feature = "embed"))]
                DaemonAction::Start { .. } => {
                    anyhow::bail!(
                        "`brain daemon start` requires the `embed` feature \u{2014} \
                         this binary was built with `--no-default-features` and has no \
                         indexer. Rebuild with `cargo install brain --features embed`."
                    );
                }
                DaemonAction::Stop => daemon.stop()?,
                DaemonAction::Status => {
                    daemon.status()?;
                    // Also show service status if installed
                    let brain_root = resolve_brain_root()?;
                    if let Some(root) = brain_root {
                        println!();
                        commands::daemon_service::status(&root)?;
                    }
                }
                DaemonAction::Install {
                    brain_root,
                    dry_run,
                } => {
                    let root = brain_root
                        .or_else(|| resolve_brain_root().ok().flatten())
                        .context("No brain found. Run from a directory with .brain/brain.toml or pass --brain-root.")?;
                    commands::daemon_service::install(&root, dry_run)?;
                }
                DaemonAction::Uninstall { brain_root } => {
                    let root = brain_root
                        .or_else(|| resolve_brain_root().ok().flatten())
                        .context("No brain found. Run from a directory with .brain/brain.toml or pass --brain-root.")?;
                    commands::daemon_service::uninstall(&root)?;
                }
            }
        }
        #[cfg(feature = "embed")]
        Command::Reindex { full, file } => match (full, file) {
            (Some(notes_path), None) => {
                commands::reindex::run_full(notes_path, cli.model_dir, cli.lance_db, cli.sqlite_db)
                    .await?
            }
            (None, Some(file_path)) => {
                commands::reindex::run_file(file_path, cli.model_dir, cli.lance_db, cli.sqlite_db)
                    .await?
            }
            (Some(_), Some(_)) => {
                anyhow::bail!("Cannot specify both --full and --file");
            }
            (None, None) => {
                anyhow::bail!("Must specify either --full <path> or --file <path>");
            }
        },
        #[cfg(feature = "embed")]
        Command::Vacuum { older_than } => {
            commands::vacuum::run(cli.model_dir, cli.lance_db, cli.sqlite_db, older_than).await?
        }
        #[cfg(feature = "embed")]
        Command::BackfillTasks { dry_run } => {
            commands::backfill_tasks::run(cli.model_dir, cli.lance_db, cli.sqlite_db, dry_run)
                .await?
        }
        #[cfg(feature = "embed")]
        Command::Doctor { notes_path } => {
            commands::doctor::run(notes_path, cli.model_dir, cli.lance_db, cli.sqlite_db).await?
        }
        Command::Mcp { action } => match action {
            None => {
                commands::mcp::run(cli.model_dir, cli.lance_db, cli.sqlite_db).await?;
            }
            Some(McpAction::Setup { target, dry_run }) => {
                commands::mcp_setup::run(target, dry_run)?;
            }
        },
        Command::Plugin { action } => match action {
            PluginAction::Install { target } => {
                commands::plugin::install(target)?;
            }
            PluginAction::Uninstall { target } => {
                commands::plugin::uninstall(target)?;
            }
        },
        Command::Hooks { action } => match action {
            HooksAction::Install { dry_run } => {
                commands::hooks::install(dry_run)?;
            }
            HooksAction::Status => {
                commands::hooks::status()?;
            }
            HooksAction::PreCompact => {
                commands::hooks::pre_compact()?;
            }
            HooksAction::Stop => {
                commands::hooks::stop()?;
            }
            HooksAction::PreToolUse => {
                commands::hooks::pre_tool_use()?;
            }
            HooksAction::SessionStart => {
                commands::hooks::session_start()?;
            }
            HooksAction::UserPromptSubmit => {
                commands::hooks::user_prompt_submit()?;
            }
        },
        Command::Docs => {
            commands::docs::run()?;
        }
        Command::Id => {
            commands::id::run()?;
        }
        Command::Agent { action } => match action {
            AgentAction::Schema { tool, pretty } => {
                commands::agent_schema::run(tool, pretty)?;
            }
        },
        Command::ImportBeads { path, dry_run } => {
            commands::import_beads::run(path, cli.sqlite_db, Some(cli.lance_db), dry_run)?;
        }
        Command::Init {
            name,
            notes,
            no_agents_md,
        } => {
            commands::init::run(name, notes, no_agents_md)?;
        }
        Command::Link { name } => {
            commands::link::run(&name)?;
        }
        Command::List {
            json,
            all,
            archived,
        } => {
            commands::registry::run_list(&cli.sqlite_db, json, all, archived)?;
        }
        Command::Remove { name, purge } => {
            commands::registry::run_remove(&name, purge)?;
        }
        Command::Alias { action } => match action {
            AliasAction::Add { brain, alias } => {
                commands::alias::run_add(&brain, &alias)?;
            }
            AliasAction::Remove { brain, alias } => {
                commands::alias::run_remove(&brain, &alias)?;
            }
            AliasAction::List { brain } => {
                commands::alias::run_list(brain.as_deref())?;
            }
        },
        Command::Config { action } => {
            // Derive brain name from the LanceDB path (per-brain), since sqlite_db
            // now points to the unified ~/.brain/brain.db.
            let brain_name = cli
                .lance_db
                .parent()
                .and_then(|p| p.file_name())
                .and_then(|n| n.to_str())
                .unwrap_or("brain")
                .to_string();
            match action {
                ConfigAction::Get { key } => {
                    commands::config::run_config_get(&cli.sqlite_db, &brain_name, &key)?;
                }
                ConfigAction::Set { key, value } => {
                    commands::config::run_config_set(&cli.sqlite_db, &brain_name, &key, value)?;
                }
                ConfigAction::Provider { action } => match action {
                    ProviderAction::Set { name, api_key } => {
                        commands::provider::run_set(
                            &cli.sqlite_db,
                            Some(&cli.lance_db),
                            &name,
                            api_key.as_deref(),
                        )?;
                    }
                    ProviderAction::List { remote } => {
                        commands::provider::run_list(&cli.sqlite_db, Some(&cli.lance_db), remote)?;
                    }
                    ProviderAction::Remove { target } => {
                        commands::provider::run_remove(
                            &cli.sqlite_db,
                            Some(&cli.lance_db),
                            &target,
                        )?;
                    }
                },
            }
        }
        Command::Sagas { json, action } => {
            let ctx = commands::sagas::SagaCtx::new(&cli.sqlite_db, json)?;
            match action {
                SagasAction::Create {
                    title,
                    description,
                    remote,
                } => {
                    commands::sagas::create(&ctx, &title, description.as_deref(), remote)?;
                }
                SagasAction::Show { saga_id, remote } => {
                    commands::sagas::show(&ctx, &saga_id, remote)?;
                }
                SagasAction::List {
                    include_closed,
                    include_cancelled,
                    all,
                    containing_brain,
                    remote,
                } => {
                    commands::sagas::list(
                        &ctx,
                        include_closed,
                        include_cancelled,
                        all,
                        containing_brain,
                        remote,
                    )?;
                }
                SagasAction::Update {
                    saga_id,
                    title,
                    description,
                    clear_description,
                    remote,
                } => {
                    // Map CLI flags to the store's Option<Option<&str>> description convention:
                    //   --clear-description  => Some(None)   (set NULL)
                    //   --description "x"   => Some(Some("x"))
                    //   (neither)           => None           (leave unchanged)
                    let desc_arg = if clear_description {
                        Some(None)
                    } else {
                        description.as_deref().map(Some)
                    };
                    commands::sagas::update(&ctx, &saga_id, title.as_deref(), desc_arg, remote)?;
                }
                SagasAction::Add {
                    saga_id,
                    task_ids,
                    cascade,
                    remote,
                } => {
                    commands::sagas::add_tasks(&ctx, &saga_id, &task_ids, cascade, remote)?;
                }
                SagasAction::Start { saga_id, remote } => {
                    commands::sagas::start(&ctx, &saga_id, remote)?;
                }
                SagasAction::Remove {
                    saga_id,
                    task_ids,
                    cascade,
                    remote,
                } => {
                    commands::sagas::remove(&ctx, &saga_id, task_ids, cascade, remote)?;
                }
                SagasAction::Close {
                    saga_id,
                    cascade,
                    remote,
                } => {
                    commands::sagas::close(&ctx, &saga_id, cascade, remote)?;
                }
                SagasAction::Reopen { saga_id, remote } => {
                    commands::sagas::reopen(&ctx, &saga_id, remote)?;
                }
                SagasAction::Frontier { saga_id, remote } => {
                    commands::sagas::frontier(&ctx, &saga_id, remote)?;
                }
                SagasAction::Stats { saga_id, remote } => {
                    commands::sagas::stats(&ctx, &saga_id, remote)?;
                }
                SagasAction::Cancel {
                    saga_id,
                    cascade,
                    remote,
                } => {
                    commands::sagas::cancel(&ctx, &saga_id, cascade, remote)?;
                }
            }
        }
        Command::Tasks {
            output: output_arg,
            json,
            markdown: _,
            action,
        } => {
            use commands::tasks::run::{
                CreateParams, ListParams, NextParams, ShowParams, TaskCtx, UpdateParams,
            };
            let output = match output_arg {
                Some(OutputFormatArg::HookEnvelope) => OutputFormat::HookEnvelope,
                Some(OutputFormatArg::Json) => OutputFormat::Json,
                Some(OutputFormatArg::Human) => OutputFormat::Human,
                None if json => OutputFormat::Json,
                None => OutputFormat::Human,
            };
            let ctx = TaskCtx::new(&cli.sqlite_db, Some(&cli.lance_db), output)?;

            match action {
                TasksAction::Create {
                    title,
                    description,
                    priority,
                    task_type,
                    assignee,
                    parent,
                    brain,
                    remote,
                } => {
                    commands::tasks::run::create(
                        &ctx,
                        CreateParams {
                            title,
                            description,
                            priority,
                            task_type: task_type.into(),
                            assignee,
                            parent,
                            brain,
                            remote,
                        },
                    )?;
                }
                TasksAction::List {
                    status,
                    priority,
                    task_type,
                    assignee,
                    label,
                    search,
                    ready,
                    blocked,
                    include_description,
                    group_by,
                    brain,
                    remote,
                } => {
                    let params = ListParams {
                        status,
                        priority,
                        task_type: task_type.map(Into::into),
                        assignee,
                        label,
                        search,
                        ready,
                        blocked,
                        include_description,
                        group_by,
                        brain,
                        remote,
                    };
                    commands::tasks::run::list(&ctx, &params)?;
                }
                TasksAction::Show { id, brain, remote } => {
                    commands::tasks::run::show(&ctx, ShowParams { id, brain, remote })?;
                }
                TasksAction::Update {
                    id,
                    title,
                    description,
                    status,
                    priority,
                    task_type,
                    assignee,
                    blocked_reason,
                    remote,
                } => {
                    commands::tasks::run::update(
                        &ctx,
                        UpdateParams {
                            id,
                            title,
                            description,
                            status,
                            priority,
                            task_type: task_type.map(Into::into),
                            assignee,
                            blocked_reason,
                            remote,
                        },
                    )?;
                }
                TasksAction::Dep { action } => match action {
                    DepAction::Add {
                        task_id,
                        depends_on,
                        remote,
                    } => {
                        commands::tasks::run::dep_add(&ctx, &task_id, &depends_on, remote)?;
                    }
                    DepAction::Remove {
                        task_id,
                        depends_on,
                        remote,
                    } => {
                        commands::tasks::run::dep_remove(&ctx, &task_id, &depends_on, remote)?;
                    }
                    DepAction::AddChain { task_ids } => {
                        commands::tasks::run::dep_add_chain(&ctx, &task_ids)?;
                    }
                    DepAction::AddFan { source, dependents } => {
                        commands::tasks::run::dep_add_fan(&ctx, &source, &dependents)?;
                    }
                    DepAction::Clear { task_id } => {
                        commands::tasks::run::dep_clear(&ctx, &task_id)?;
                    }
                },
                TasksAction::Link { task_id, chunk_id } => {
                    commands::tasks::run::link(&ctx, &task_id, &chunk_id)?;
                }
                TasksAction::Unlink { task_id, chunk_id } => {
                    commands::tasks::run::unlink(&ctx, &task_id, &chunk_id)?;
                }
                TasksAction::ExtLink { action } => match action {
                    ExtLinkAction::Add {
                        task_id,
                        source,
                        id,
                        url,
                    } => {
                        commands::tasks::run::ext_link_add(
                            &ctx,
                            &task_id,
                            &source,
                            &id,
                            url.as_deref(),
                        )?;
                    }
                    ExtLinkAction::Remove {
                        task_id,
                        source,
                        id,
                    } => {
                        commands::tasks::run::ext_link_remove(&ctx, &task_id, &source, &id)?;
                    }
                    ExtLinkAction::List { task_id } => {
                        commands::tasks::run::ext_link_list(&ctx, &task_id)?;
                    }
                },
                TasksAction::Comment { task_id, body } => {
                    commands::tasks::run::comment(&ctx, &task_id, &body)?;
                }
                TasksAction::Label { action } => match action {
                    LabelAction::Add {
                        task_id,
                        label,
                        brain,
                        remote,
                    } => {
                        commands::tasks::run::label_add(
                            &ctx,
                            &task_id,
                            &label,
                            brain.as_deref(),
                            remote,
                        )?;
                    }
                    LabelAction::Remove {
                        task_id,
                        label,
                        brain,
                        remote,
                    } => {
                        commands::tasks::run::label_remove(
                            &ctx,
                            &task_id,
                            &label,
                            brain.as_deref(),
                            remote,
                        )?;
                    }
                    LabelAction::BatchAdd {
                        tasks,
                        label,
                        brain,
                    } => {
                        commands::tasks::run::label_batch_add(
                            &ctx,
                            &tasks,
                            &label,
                            brain.as_deref(),
                        )?;
                    }
                    LabelAction::BatchRemove {
                        tasks,
                        label,
                        brain,
                    } => {
                        commands::tasks::run::label_batch_remove(
                            &ctx,
                            &tasks,
                            &label,
                            brain.as_deref(),
                        )?;
                    }
                    LabelAction::Rename {
                        old_label,
                        new_label,
                    } => {
                        commands::tasks::run::label_rename(&ctx, &old_label, &new_label)?;
                    }
                    LabelAction::Purge { label } => {
                        commands::tasks::run::label_purge(&ctx, &label)?;
                    }
                },
                TasksAction::Export { format, dir } => match format.as_str() {
                    "markdown" | "md" => {
                        commands::tasks::export_markdown::run(
                            dir,
                            cli.sqlite_db,
                            Some(cli.lance_db),
                        )?;
                    }
                    other => {
                        anyhow::bail!("Unknown export format: {other}. Supported: markdown");
                    }
                },
                TasksAction::Close { ids, brain } => {
                    commands::tasks::run::close(&ctx, &ids, brain.as_deref())?;
                }
                TasksAction::Ready => {
                    commands::tasks::run::list(
                        &ctx,
                        &ListParams {
                            status: None,
                            priority: None,
                            task_type: None,
                            assignee: None,
                            label: None,
                            search: None,
                            ready: true,
                            blocked: false,
                            include_description: false,
                            group_by: None,
                            brain: None,
                            remote: false,
                        },
                    )?;
                }
                TasksAction::Blocked => {
                    commands::tasks::run::list(
                        &ctx,
                        &ListParams {
                            status: None,
                            priority: None,
                            task_type: None,
                            assignee: None,
                            label: None,
                            search: None,
                            ready: false,
                            blocked: true,
                            include_description: false,
                            group_by: None,
                            brain: None,
                            remote: false,
                        },
                    )?;
                }
                TasksAction::Stats => {
                    commands::tasks::run::stats(&ctx)?;
                }
                TasksAction::Labels => {
                    commands::tasks::run::labels(&ctx)?;
                }
                TasksAction::Next { k, remote } => {
                    commands::tasks::run::next(&ctx, NextParams { k, remote })?;
                }
                TasksAction::Transfer {
                    task_id,
                    to,
                    dry_run,
                    remote,
                } => {
                    use commands::tasks::run::TransferParams;
                    // Open a writable LanceDB handle so vector rows are
                    // re-stamped to the target brain. If the open fails (e.g.
                    // path missing in tests), proceed without — `transfer_task`
                    // tolerates `None` and logs a warning if vectors drift.
                    let vector_store = if dry_run || remote {
                        None
                    } else {
                        brain_persistence::store::Store::open_or_create(&cli.lance_db)
                            .await
                            .ok()
                    };
                    commands::tasks::run::transfer(
                        &ctx,
                        TransferParams {
                            task_id,
                            to,
                            dry_run,
                            remote,
                        },
                        vector_store.as_ref(),
                    )
                    .await?;
                }
            }
        }
        Command::Snapshots { json, action } => {
            use commands::snapshots::run::{ListParams, SaveParams, SnapshotCtx};
            let ctx = SnapshotCtx::new(&cli.sqlite_db, Some(&cli.lance_db), json)?;

            match action {
                SnapshotsAction::Save {
                    title,
                    file,
                    stdin,
                    description,
                    task,
                    tag,
                    media_type,
                    brain,
                    remote,
                } => {
                    commands::snapshots::run::save(
                        &ctx,
                        SaveParams {
                            title,
                            file,
                            stdin,
                            description,
                            task,
                            tags: tag,
                            media_type,
                            brain,
                            remote,
                        },
                    )?;
                }
                SnapshotsAction::List {
                    tag,
                    status,
                    limit,
                    remote,
                } => {
                    commands::snapshots::run::list(
                        &ctx,
                        &ListParams {
                            tag,
                            status,
                            limit,
                            remote,
                        },
                    )?;
                }
                SnapshotsAction::Get { id } => {
                    commands::snapshots::run::get(&ctx, &id)?;
                }
                SnapshotsAction::Restore { id, output } => {
                    commands::snapshots::run::restore(&ctx, &id, output)?;
                }
                SnapshotsAction::Archive { id, reason } => {
                    commands::snapshots::run::archive(&ctx, &id, reason)?;
                }
                SnapshotsAction::Tag { action } => match action {
                    RecordTagAction::Add { id, tag } => {
                        commands::snapshots::run::tag_add(&ctx, &id, &tag)?;
                    }
                    RecordTagAction::Remove { id, tag } => {
                        commands::snapshots::run::tag_remove(&ctx, &id, &tag)?;
                    }
                },
                SnapshotsAction::Link { action } => match action {
                    RecordLinkAction::Add { id, task, chunk } => {
                        commands::snapshots::run::link_add(&ctx, &id, task, chunk)?;
                    }
                    RecordLinkAction::Remove { id, task, chunk } => {
                        commands::snapshots::run::link_remove(&ctx, &id, task, chunk)?;
                    }
                },
            }
        }
        Command::Artifacts { json, action } => {
            use commands::artifacts::run::ArtifactCtx;
            let ctx = ArtifactCtx::new(&cli.sqlite_db, Some(&cli.lance_db), json)?;

            match action {
                ArtifactsAction::List {
                    kind,
                    tag,
                    status,
                    limit,
                    remote,
                } => {
                    commands::artifacts::run::list(
                        &ctx,
                        &commands::artifacts::run::ListParams {
                            kind,
                            tag,
                            status,
                            limit,
                            remote,
                        },
                    )?;
                }
                ArtifactsAction::Get { id } => {
                    commands::artifacts::run::get(&ctx, &id)?;
                }
                ArtifactsAction::Archive { id, reason } => {
                    commands::artifacts::run::archive(&ctx, &id, reason)?;
                }
                ArtifactsAction::Tag { action } => match action {
                    RecordTagAction::Add { id, tag } => {
                        commands::artifacts::run::tag_add(&ctx, &id, &tag)?;
                    }
                    RecordTagAction::Remove { id, tag } => {
                        commands::artifacts::run::tag_remove(&ctx, &id, &tag)?;
                    }
                },
                ArtifactsAction::Link { action } => match action {
                    RecordLinkAction::Add { id, task, chunk } => {
                        commands::artifacts::run::link_add(&ctx, &id, task, chunk)?;
                    }
                    RecordLinkAction::Remove { id, task, chunk } => {
                        commands::artifacts::run::link_remove(&ctx, &id, task, chunk)?;
                    }
                },
                ArtifactsAction::Restore { id, output } => {
                    commands::artifacts::run::restore(&ctx, &id, output)?;
                }
            }
        }
        Command::Documents { json, action } => {
            let ctx = commands::documents::run::DocumentCtx::new(
                &cli.sqlite_db,
                Some(&cli.lance_db),
                json,
            )?;
            match action {
                DocumentsAction::Create {
                    title,
                    file,
                    stdin,
                    text,
                    description,
                    task,
                    tag,
                    media_type,
                    brain,
                    remote,
                } => {
                    commands::documents::run::create(
                        &ctx,
                        commands::documents::run::CreateParams {
                            title,
                            file,
                            stdin,
                            text,
                            description,
                            task,
                            tags: tag,
                            media_type,
                            brain,
                            remote,
                        },
                    )?;
                }
            }
        }
        Command::Analyses { json, action } => {
            let ctx = commands::analyses::run::AnalysisCtx::new(
                &cli.sqlite_db,
                Some(&cli.lance_db),
                json,
            )?;
            match action {
                AnalysesAction::Create {
                    title,
                    file,
                    stdin,
                    text,
                    description,
                    task,
                    tag,
                    media_type,
                    brain,
                    remote,
                } => {
                    commands::analyses::run::create(
                        &ctx,
                        commands::analyses::run::CreateParams {
                            title,
                            file,
                            stdin,
                            text,
                            description,
                            task,
                            tags: tag,
                            media_type,
                            brain,
                            remote,
                        },
                    )?;
                }
            }
        }
        Command::Plans { json, action } => {
            let ctx =
                commands::plans::run::PlanCtx::new(&cli.sqlite_db, Some(&cli.lance_db), json)?;
            match action {
                PlansAction::Create {
                    title,
                    file,
                    stdin,
                    text,
                    description,
                    task,
                    tag,
                    media_type,
                    brain,
                    remote,
                } => {
                    commands::plans::run::create(
                        &ctx,
                        commands::plans::run::CreateParams {
                            title,
                            file,
                            stdin,
                            text,
                            description,
                            task,
                            tags: tag,
                            media_type,
                            brain,
                            remote,
                        },
                    )?;
                }
            }
        }
        Command::Tags { json, action } => {
            use commands::tags::run::{AliasesListParams, TagsCtx};
            let ctx = TagsCtx::new(&cli.sqlite_db, Some(&cli.lance_db), json)?;
            match action {
                #[cfg(feature = "embed")]
                TagsAction::Recluster { threshold } => {
                    commands::tags::run::recluster(&ctx, &cli.model_dir, threshold).await?;
                }
                TagsAction::Aliases { action } => match action {
                    AliasesAction::List {
                        canonical,
                        cluster_id,
                        limit,
                        offset,
                        remote,
                    } => {
                        commands::tags::run::aliases_list(
                            &ctx,
                            AliasesListParams {
                                canonical,
                                cluster_id,
                                limit,
                                offset,
                                remote,
                            },
                        )?;
                    }
                },
                TagsAction::Status { remote } => {
                    commands::tags::run::status(&ctx, Some(&cli.model_dir), remote)?;
                }
            }
        }
        Command::Migrate {
            yes,
            cleanup,
            brain,
        } => {
            commands::migrate::run(commands::migrate::MigrateArgs {
                yes,
                cleanup,
                brain,
            })?;
        }
        Command::Records { json, action } => {
            use commands::records::RecordsCtx;

            match action {
                #[cfg(feature = "embed")]
                RecordsAction::Search {
                    query,
                    k,
                    budget,
                    tags,
                    brains,
                } => {
                    use commands::memory::run::MemoryCtx;
                    use commands::records::{RecordsSearchParams, search};
                    let ctx =
                        MemoryCtx::new(&cli.sqlite_db, &cli.lance_db, &cli.model_dir, json).await?;
                    search(
                        &ctx,
                        RecordsSearchParams {
                            query,
                            k,
                            budget,
                            tags,
                            brains,
                        },
                    )
                    .await?;
                }
                action => {
                    let ctx = RecordsCtx::new(&cli.sqlite_db, Some(&cli.lance_db), json)?;
                    match action {
                        RecordsAction::Verify { verbose, remote } => {
                            commands::records::verify(&ctx, verbose, remote)?;
                        }
                        RecordsAction::Gc { dry_run } => {
                            commands::records::gc(&ctx, dry_run)?;
                        }
                        RecordsAction::Evict { id, reason } => {
                            commands::records::evict(&ctx, &id, reason)?;
                        }
                        RecordsAction::Pin { id } => {
                            commands::records::pin(&ctx, &id)?;
                        }
                        RecordsAction::Unpin { id } => {
                            commands::records::unpin(&ctx, &id)?;
                        }
                        #[cfg(feature = "embed")]
                        RecordsAction::Search { .. } => unreachable!(),
                    }
                }
            }
        }
        #[cfg(feature = "embed")]
        Command::Memory { json, action } => {
            use commands::memory::run::{
                MemoryCtx, ReflectCommitParams, ReflectPrepareParams, RetrieveParams,
                WriteEpisodeParams, WriteProcedureParams,
            };

            match action {
                MemoryAction::Consolidate {
                    limit,
                    gap_seconds,
                    auto_summarize,
                    remote,
                } => {
                    if remote {
                        commands::memory::run::consolidate_remote(
                            limit,
                            gap_seconds,
                            auto_summarize,
                            json,
                        )?;
                    } else {
                        let ctx =
                            MemoryCtx::new(&cli.sqlite_db, &cli.lance_db, &cli.model_dir, json)
                                .await?;
                        commands::memory::run::consolidate(
                            &ctx,
                            limit,
                            gap_seconds,
                            auto_summarize,
                        )
                        .await?;
                    }
                }
                MemoryAction::Retrieve {
                    query,
                    uri,
                    lod,
                    count,
                    strategy,
                    brains,
                    time_scope,
                    time_after,
                    time_before,
                    tags,
                    tags_require,
                    tags_exclude,
                    kinds,
                    explain,
                    remote,
                } => {
                    if remote {
                        commands::memory::run::retrieve_remote(
                            RetrieveParams {
                                query,
                                uri,
                                lod,
                                count,
                                strategy,
                                brains,
                                time_scope,
                                time_after,
                                time_before,
                                tags,
                                tags_require,
                                tags_exclude,
                                kinds,
                                explain,
                            },
                            json,
                        )?;
                    } else {
                        let ctx =
                            MemoryCtx::new(&cli.sqlite_db, &cli.lance_db, &cli.model_dir, json)
                                .await?;
                        commands::memory::run::retrieve(
                            &ctx,
                            RetrieveParams {
                                query,
                                uri,
                                lod,
                                count,
                                strategy,
                                brains,
                                time_scope,
                                time_after,
                                time_before,
                                tags,
                                tags_require,
                                tags_exclude,
                                kinds,
                                explain,
                            },
                        )
                        .await?;
                    }
                }
                MemoryAction::WriteEpisode {
                    goal,
                    actions,
                    outcome,
                    tags,
                    importance,
                    remote,
                } => {
                    if remote {
                        commands::memory::run::write_episode_remote(
                            WriteEpisodeParams {
                                goal,
                                actions,
                                outcome,
                                tags,
                                importance,
                                lance_db: None,
                            },
                            json,
                        )?;
                    } else {
                        let ctx =
                            MemoryCtx::new(&cli.sqlite_db, &cli.lance_db, &cli.model_dir, json)
                                .await?;
                        commands::memory::run::write_episode(
                            &ctx,
                            WriteEpisodeParams {
                                goal,
                                actions,
                                outcome,
                                tags,
                                importance,
                                lance_db: Some(cli.lance_db.clone()),
                            },
                        )
                        .await?;
                    }
                }
                MemoryAction::WriteProcedure {
                    title,
                    steps,
                    tags,
                    importance,
                    remote,
                } => {
                    if remote {
                        commands::memory::run::write_procedure_remote(
                            WriteProcedureParams {
                                title,
                                steps,
                                tags,
                                importance,
                                lance_db: None,
                            },
                            json,
                        )?;
                    } else {
                        let ctx =
                            MemoryCtx::new(&cli.sqlite_db, &cli.lance_db, &cli.model_dir, json)
                                .await?;
                        commands::memory::run::write_procedure(
                            &ctx,
                            WriteProcedureParams {
                                title,
                                steps,
                                tags,
                                importance,
                                lance_db: Some(cli.lance_db.clone()),
                            },
                        )
                        .await?;
                    }
                }
                MemoryAction::SummarizeScope {
                    scope_type,
                    scope_value,
                    regenerate,
                    async_llm,
                    remote,
                } => {
                    if remote {
                        commands::memory::run::summarize_scope_remote(
                            &scope_type,
                            &scope_value,
                            regenerate,
                            async_llm,
                            json,
                        )?;
                    } else {
                        let ctx =
                            MemoryCtx::new(&cli.sqlite_db, &cli.lance_db, &cli.model_dir, json)
                                .await?;
                        commands::memory::run::summarize_scope(
                            &ctx,
                            &scope_type,
                            &scope_value,
                            regenerate,
                            async_llm,
                        )
                        .await?;
                    }
                }
                MemoryAction::Reflect {
                    commit,
                    topic,
                    budget,
                    brains,
                    title,
                    content,
                    source_ids,
                    tags,
                    importance,
                    remote,
                } => {
                    if remote {
                        commands::memory::run::reflect_remote(
                            commit, topic, budget, brains, title, content, source_ids, tags,
                            importance, json,
                        )?;
                    } else {
                        let ctx =
                            MemoryCtx::new(&cli.sqlite_db, &cli.lance_db, &cli.model_dir, json)
                                .await?;
                        if commit {
                            commands::memory::run::reflect_commit(
                                &ctx,
                                ReflectCommitParams {
                                    title: title.unwrap_or_default(),
                                    content: content.unwrap_or_default(),
                                    source_ids,
                                    tags,
                                    importance: importance.unwrap_or(1.0),
                                    lance_db: Some(cli.lance_db.clone()),
                                },
                            )
                            .await?;
                        } else {
                            let topic = topic.ok_or_else(|| {
                                anyhow::anyhow!(
                                    "--topic is required in prepare mode (omit --commit)"
                                )
                            })?;
                            commands::memory::run::reflect_prepare(
                                &ctx,
                                ReflectPrepareParams {
                                    topic,
                                    budget,
                                    brains,
                                },
                            )
                            .await?;
                        }
                    }
                }
            }
        }
        Command::Status { json, remote } => {
            commands::status::run(&cli.sqlite_db, Some(&cli.lance_db), json, remote)?;
        }
        Command::Jobs { action } => match action {
            JobsAction::Status { json, remote } => {
                commands::jobs::run_status(&cli.sqlite_db, Some(&cli.lance_db), json, remote)?;
            }
            JobsAction::Retry { job_id } => {
                commands::jobs::run_retry(&cli.sqlite_db, Some(&cli.lance_db), &job_id)?;
            }
            JobsAction::Gc { older_than_days } => {
                commands::jobs::run_gc(&cli.sqlite_db, Some(&cli.lance_db), older_than_days)?;
            }
        },
    }

    Ok(())
}
