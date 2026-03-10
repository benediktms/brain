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
                    let outcome = commands::watch::run(
                        notes_path,
                        cli.model_dir,
                        cli.lance_db,
                        cli.sqlite_db,
                    )
                    .await?;
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
        Command::Agent { action } => match action {
            AgentAction::Schema { tool, pretty } => {
                commands::agent_schema::run(tool, pretty)?;
            }
        },
        Command::ImportBeads { path, dry_run } => {
            commands::import_beads::run(path, cli.sqlite_db, dry_run)?;
        }
        Command::Init {
            name,
            notes,
            no_agents_md,
        } => {
            commands::init::run(name, notes, no_agents_md)?;
        }
        Command::List => {
            commands::list::run_list()?;
        }
        Command::Remove { name, purge } => {
            commands::list::run_remove(&name, purge)?;
        }
        Command::Config { action } => match action {
            ConfigAction::Set { key, value } if key == "prefix" => {
                let brain_dir = cli.sqlite_db.parent().unwrap_or(std::path::Path::new("."));

                // Get the old prefix and compute the new one
                let db = brain_lib::db::Db::open(&cli.sqlite_db)?;
                let (old_prefix, new_prefix) = db.with_write_conn(|conn| {
                    let old = brain_lib::db::meta::get_or_init_project_prefix(conn, brain_dir)?;

                    let new = match value {
                        Some(v) => {
                            let upper = v.to_ascii_uppercase();
                            if upper.len() != 3 || !upper.chars().all(|c| c.is_ascii_uppercase()) {
                                return Err(brain_lib::error::BrainCoreError::Config(format!(
                                    "prefix must be exactly 3 uppercase ASCII letters, got: {v}"
                                )));
                            }
                            upper
                        }
                        None => {
                            let name = brain_dir
                                .file_name()
                                .and_then(|n| n.to_str())
                                .unwrap_or("BRN");
                            brain_lib::db::meta::generate_prefix(name)
                        }
                    };

                    brain_lib::db::meta::set_meta(conn, "project_prefix", &new)?;
                    Ok((old, new))
                })?;
                drop(db);

                if old_prefix == new_prefix {
                    println!("Prefix is already {new_prefix}");
                } else {
                    // Rewrite task IDs in the event log
                    let tasks_dir = brain_dir.join("tasks");
                    let db2 = brain_lib::db::Db::open(&cli.sqlite_db)?;
                    let store = brain_lib::tasks::TaskStore::new(&tasks_dir, db2)?;
                    let count = store.rewrite_prefix(&old_prefix, &new_prefix)?;
                    println!("Rewrote {count} events: {old_prefix} → {new_prefix}");
                }
            }
            action => {
                let db = brain_lib::db::Db::open(&cli.sqlite_db)?;
                db.with_write_conn(|conn| match action {
                    ConfigAction::Set { key, .. } => {
                        let other = key.as_str();
                        Err(brain_lib::error::BrainCoreError::Config(format!(
                            "unknown config key: {other}. Known keys: prefix"
                        )))
                    }
                    ConfigAction::Get { key } => match key.as_str() {
                        "prefix" => {
                            let brain_dir =
                                cli.sqlite_db.parent().unwrap_or(std::path::Path::new("."));
                            let prefix =
                                brain_lib::db::meta::get_or_init_project_prefix(conn, brain_dir)?;
                            println!("{prefix}");
                            Ok(())
                        }
                        other => Err(brain_lib::error::BrainCoreError::Config(format!(
                            "unknown config key: {other}. Known keys: prefix"
                        ))),
                    },
                })?;
            }
        },
        Command::Tasks {
            json,
            markdown: _,
            action,
        } => {
            use commands::tasks::run::{CreateParams, ListParams, TaskCtx, UpdateParams};
            let ctx = TaskCtx::new(&cli.sqlite_db, json)?;

            match action {
                TasksAction::Create {
                    title,
                    description,
                    priority,
                    task_type,
                    assignee,
                    parent,
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
                    };
                    commands::tasks::run::list(&ctx, &params)?;
                }
                TasksAction::Show { id } => {
                    commands::tasks::run::show(&ctx, &id)?;
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
                TasksAction::Comment { task_id, body } => {
                    commands::tasks::run::comment(&ctx, &task_id, &body)?;
                }
                TasksAction::Label { action } => match action {
                    LabelAction::Add { task_id, label } => {
                        commands::tasks::run::label_add(&ctx, &task_id, &label)?;
                    }
                    LabelAction::Remove { task_id, label } => {
                        commands::tasks::run::label_remove(&ctx, &task_id, &label)?;
                    }
                    LabelAction::BatchAdd { tasks, label } => {
                        commands::tasks::run::label_batch_add(&ctx, &tasks, &label)?;
                    }
                    LabelAction::BatchRemove { tasks, label } => {
                        commands::tasks::run::label_batch_remove(&ctx, &tasks, &label)?;
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
                        commands::tasks::export_markdown::run(dir, cli.sqlite_db)?;
                    }
                    other => {
                        anyhow::bail!("Unknown export format: {other}. Supported: markdown");
                    }
                },
                TasksAction::Close { ids } => {
                    commands::tasks::run::close(&ctx, &ids)?;
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
                        },
                    )?;
                }
                TasksAction::Stats => {
                    commands::tasks::run::stats(&ctx)?;
                }
                TasksAction::Labels => {
                    commands::tasks::run::labels(&ctx)?;
                }
            }
        }
    }

    Ok(())
}
