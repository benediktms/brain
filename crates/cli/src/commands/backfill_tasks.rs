use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use brain_lib::prelude::*;

/// Backfill task capsule embeddings into the vector store for tasks
/// that were created before the automatic embedding feature.
pub async fn run(
    model_dir: PathBuf,
    db_path: PathBuf,
    sqlite_path: PathBuf,
    dry_run: bool,
) -> Result<()> {
    // Open Db, Store, Embedder
    let db = tokio::task::spawn_blocking({
        let sqlite_path = sqlite_path.clone();
        move || Db::open(&sqlite_path)
    })
    .await??;

    let mut store = Store::open_or_create(&db_path).await?;
    brain_lib::pipeline::ensure_schema_version(&db, &mut store).await?;

    let embedder: Arc<dyn Embed> = {
        let model_dir = model_dir.clone();
        Arc::new(tokio::task::spawn_blocking(move || Embedder::load(&model_dir)).await??)
    };

    // Open TaskStore
    let task_store = brain_lib::tasks::TaskStore::new(db.clone());

    // List all tasks
    let all_tasks = task_store.list_all()?;
    if all_tasks.is_empty() {
        println!("No tasks found.");
        return Ok(());
    }

    // Batch-fetch labels for all tasks
    let task_ids: Vec<&str> = all_tasks.iter().map(|t| t.task_id.as_str()).collect();
    let labels_map = task_store.get_labels_for_tasks(&task_ids)?;

    let total = all_tasks.len();
    let terminal_count = all_tasks
        .iter()
        .filter(|t| t.status == "done" || t.status == "cancelled")
        .count();

    if dry_run {
        println!(
            "Dry run: would backfill {total} task capsules + {terminal_count} outcome capsules"
        );
        for task in &all_tasks {
            let labels = labels_map
                .get(&task.task_id)
                .map(|v| v.join(", "))
                .unwrap_or_default();
            println!(
                "  {} [{}] {} {}",
                task.task_id,
                task.status,
                task.title,
                if labels.is_empty() {
                    String::new()
                } else {
                    format!("({labels})")
                },
            );
        }
        return Ok(());
    }

    let mut embedded = 0usize;
    let mut outcomes = 0usize;
    let mut errors = 0usize;

    for (i, task) in all_tasks.iter().enumerate() {
        let labels = labels_map.get(&task.task_id).cloned().unwrap_or_default();

        // Embed task capsule
        match brain_lib::tasks::capsule::embed_task_capsule(
            &store,
            &embedder,
            &db,
            brain_lib::tasks::capsule::TaskCapsuleParams {
                task_id: &task.task_id,
                title: &task.title,
                description: task.description.as_deref(),
                labels: &labels,
                priority: task.priority,
            },
        )
        .await
        {
            Ok(()) => embedded += 1,
            Err(e) => {
                eprintln!("  warn: task capsule failed for {}: {e}", task.task_id);
                errors += 1;
            }
        }

        // Embed outcome capsule for done/cancelled tasks
        if task.status == "done" || task.status == "cancelled" {
            match brain_lib::tasks::capsule::embed_outcome_capsule(
                &store,
                &embedder,
                &db,
                &task.task_id,
                &task.title,
                None,
            )
            .await
            {
                Ok(()) => outcomes += 1,
                Err(e) => {
                    eprintln!("  warn: outcome capsule failed for {}: {e}", task.task_id);
                    errors += 1;
                }
            }
        }

        // Progress indicator every 25 tasks
        if (i + 1) % 25 == 0 || i + 1 == total {
            println!("  [{}/{}] tasks processed", i + 1, total);
        }
    }

    println!(
        "Backfill complete: {embedded} task capsules, {outcomes} outcome capsules, {errors} errors"
    );

    Ok(())
}
