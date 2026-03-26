use std::path::PathBuf;

use anyhow::{Context, Result};
use tracing_subscriber::EnvFilter;

use crate::cli::*;
use crate::commands;

// ── helpers ─────────────────────────────────────────────────

/// Try to find the brain project root by walking up from cwd.
/// Returns `Ok(Some(root))` if found, `Ok(None)` if no marker file exists.
fn resolve_brain_root() -> Result<Option<PathBuf>> {
    let cwd = std::env::current_dir()?;
    Ok(brain_lib::config::find_brain_root(&cwd))
}

// ── async dispatch ──────────────────────────────────────────

pub(crate) async fn async_main(cli: Cli) -> Result<()> {
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

    // Warn if ~/.brain has overly broad permissions.
    let _ = brain_lib::config::check_brain_home_permissions();

    match cli.command {
        Command::Index { notes_path } => {
            commands::index::run(notes_path, cli.model_dir, cli.lance_db, cli.sqlite_db).await?
        }
        Command::Query {
            query,
            k,
            intent,
            budget,
            verbose,
            brains,
        } => {
            commands::query::run(commands::query::QueryParams {
                query,
                top_k: k,
                intent: intent.as_str().to_string(),
                budget,
                verbose,
                model_dir: cli.model_dir,
                db_path: cli.lance_db,
                sqlite_path: cli.sqlite_db,
                brains,
            })
            .await?
        }
        Command::Watch { notes_path } => {
            let outcome =
                commands::watch::run(notes_path, cli.model_dir, cli.lance_db, cli.sqlite_db)
                    .await?;
            if !outcome.clean {
                std::process::exit(1);
            }
        }
        Command::Daemon { action } => {
            let daemon = commands::daemon::Daemon::new()?;
            match action {
                DaemonAction::Start { notes_path } => {
                    // Child process after fork — run watch directly.
                    let outcome = match notes_path {
                        Some(path) => {
                            commands::watch::run(path, cli.model_dir, cli.lance_db, cli.sqlite_db)
                                .await?
                        }
                        None => commands::watch::run_multi().await?,
                    };
                    if !outcome.clean {
                        std::process::exit(1);
                    }
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
        Command::Vacuum { older_than } => {
            commands::vacuum::run(cli.model_dir, cli.lance_db, cli.sqlite_db, older_than).await?
        }
        Command::BackfillTasks { dry_run } => {
            commands::backfill_tasks::run(cli.model_dir, cli.lance_db, cli.sqlite_db, dry_run)
                .await?
        }
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
        Command::Hooks { action } => match action {
            HooksAction::Install { dry_run } => {
                commands::hooks::install(dry_run)?;
            }
            HooksAction::Status => {
                commands::hooks::status()?;
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
            }
        }
        Command::Tasks {
            json,
            markdown: _,
            action,
        } => {
            use commands::tasks::run::{CreateParams, ListParams, TaskCtx, UpdateParams};
            let ctx = TaskCtx::new(&cli.sqlite_db, Some(&cli.lance_db), json)?;

            match action {
                TasksAction::Create {
                    title,
                    description,
                    priority,
                    task_type,
                    assignee,
                    parent,
                    brain,
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
                    };
                    commands::tasks::run::list(&ctx, &params)?;
                }
                TasksAction::Show { id, brain } => {
                    commands::tasks::run::show(&ctx, &id, brain.as_deref())?;
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
                        },
                    )?;
                }
                TasksAction::Dep { action } => match action {
                    DepAction::Add {
                        task_id,
                        depends_on,
                    } => {
                        commands::tasks::run::dep_add(&ctx, &task_id, &depends_on)?;
                    }
                    DepAction::Remove {
                        task_id,
                        depends_on,
                    } => {
                        commands::tasks::run::dep_remove(&ctx, &task_id, &depends_on)?;
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
                    } => {
                        commands::tasks::run::label_add(&ctx, &task_id, &label, brain.as_deref())?;
                    }
                    LabelAction::Remove {
                        task_id,
                        label,
                        brain,
                    } => {
                        commands::tasks::run::label_remove(
                            &ctx,
                            &task_id,
                            &label,
                            brain.as_deref(),
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
                        },
                    )?;
                }
                TasksAction::Stats => {
                    commands::tasks::run::stats(&ctx)?;
                }
                TasksAction::Labels => {
                    commands::tasks::run::labels(&ctx)?;
                }
                TasksAction::Next { k } => {
                    commands::tasks::run::next(&ctx, k)?;
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
                        },
                    )?;
                }
                SnapshotsAction::List { tag, status, limit } => {
                    commands::snapshots::run::list(&ctx, &ListParams { tag, status, limit })?;
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
                ArtifactsAction::Create {
                    title,
                    kind,
                    file,
                    stdin,
                    description,
                    task,
                    tag,
                    media_type,
                    brain,
                } => {
                    commands::artifacts::run::create(
                        &ctx,
                        commands::artifacts::run::CreateParams {
                            title,
                            kind,
                            file,
                            stdin,
                            description,
                            task,
                            tags: tag,
                            media_type,
                            brain,
                        },
                    )?;
                }
                ArtifactsAction::List {
                    kind,
                    tag,
                    status,
                    limit,
                } => {
                    commands::artifacts::run::list(
                        &ctx,
                        &commands::artifacts::run::ListParams {
                            kind,
                            tag,
                            status,
                            limit,
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
                        RecordsAction::Verify { verbose } => {
                            commands::records::verify(&ctx, verbose)?;
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
                        RecordsAction::Search { .. } => unreachable!(),
                    }
                }
            }
        }
        Command::Memory { json, action } => {
            use commands::memory::run::{
                MemoryCtx, ReflectCommitParams, ReflectPrepareParams, SearchParams2,
                WriteEpisodeParams, WriteProcedureParams,
            };
            let ctx = MemoryCtx::new(&cli.sqlite_db, &cli.lance_db, &cli.model_dir, json).await?;

            match action {
                MemoryAction::Consolidate {
                    limit,
                    gap_seconds,
                    auto_summarize,
                } => {
                    commands::memory::run::consolidate(&ctx, limit, gap_seconds, auto_summarize)
                        .await?;
                }
                MemoryAction::Search {
                    query,
                    k,
                    intent,
                    budget,
                    tags,
                    brains,
                    explain,
                } => {
                    commands::memory::run::search(
                        &ctx,
                        SearchParams2 {
                            query,
                            k,
                            intent: intent.as_str().to_string(),
                            budget,
                            tags,
                            brains,
                            explain,
                        },
                    )
                    .await?;
                }
                MemoryAction::Expand { memory_ids, budget } => {
                    commands::memory::run::expand(&ctx, &memory_ids, budget).await?;
                }
                MemoryAction::WriteEpisode {
                    goal,
                    actions,
                    outcome,
                    tags,
                    importance,
                } => {
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
                MemoryAction::WriteProcedure {
                    title,
                    steps,
                    tags,
                    importance,
                } => {
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
                MemoryAction::SummarizeScope {
                    scope_type,
                    scope_value,
                    regenerate,
                    async_llm,
                } => {
                    commands::memory::run::summarize_scope(
                        &ctx,
                        &scope_type,
                        &scope_value,
                        regenerate,
                        async_llm,
                    )
                    .await?;
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
                } => {
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
                            anyhow::anyhow!("--topic is required in prepare mode (omit --commit)")
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
        Command::Status { json } => {
            commands::status::run(&cli.sqlite_db, Some(&cli.lance_db), json)?;
        }
    }

    Ok(())
}
