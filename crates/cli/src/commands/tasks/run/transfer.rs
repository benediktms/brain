use anyhow::{Result, bail};
use serde_json::json;

use brain_persistence::store::Store;

use super::TaskCtx;

pub struct TransferParams {
    pub task_id: String,
    pub to: String,
    pub dry_run: bool,
    pub remote: bool,
}

async fn transfer_remote(ctx: &TaskCtx, params: &TransferParams) -> Result<()> {
    if params.dry_run {
        bail!("--dry-run is not yet supported on the --remote path");
    }

    let mut client = crate::commands::rpc_client::connect_daemon()?;

    let (task, event_id) = client
        .tasks_transfer(brain_rpc::TasksTransferParams {
            task_id: params.task_id.clone(),
            target_brain: params.to.clone(),
        })
        .map_err(|e| anyhow::anyhow!("TasksTransfer rpc failed: {e}"))?;

    if ctx.output.is_json_mode() {
        let out = json!({
            "event_id": event_id,
            "task": {
                "task_id": task.task_id,
                "title": task.title,
                "status": task.status,
                "priority": task.priority,
                "brain_id": task.brain_id,
            },
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("{} → brain '{}'", task.task_id, task.brain_id);
    }

    Ok(())
}

pub async fn transfer(
    ctx: &TaskCtx,
    params: TransferParams,
    vector_store: Option<&Store>,
) -> Result<()> {
    if params.remote {
        return transfer_remote(ctx, &params).await;
    }
    // Resolve the source task ID (may be a short prefix).
    let task_id = ctx.store.resolve_task_id(&params.task_id)?;

    // Resolve the target brain.
    let (target_brain_id, target_brain_name) = ctx.store.resolve_brain(&params.to)?;

    if params.dry_run {
        if ctx.output.is_json_mode() {
            let out = serde_json::json!({
                "dry_run": true,
                "task_id": task_id,
                "target_brain_id": target_brain_id,
                "target_brain_name": target_brain_name,
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        } else {
            println!(
                "dry-run: would transfer {} → brain '{}' ({})",
                task_id, target_brain_name, target_brain_id
            );
        }
        return Ok(());
    }

    let result = ctx
        .store
        .transfer_task(&task_id, &target_brain_id, vector_store)
        .await?;

    if result.was_no_op {
        println!(
            "no-op: task {} is already in brain '{}'",
            task_id, target_brain_name
        );
        return Ok(());
    }

    if ctx.output.is_json_mode() {
        let out = serde_json::json!({
            "task_id": task_id,
            "from_brain_id": result.from_brain_id,
            "to_brain_id": result.to_brain_id,
            "from_display_id": result.from_display_id,
            "to_display_id": result.to_display_id,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        // Human-readable: "<old_display> → <new_display>"
        println!("{} → {}", result.from_display_id, result.to_display_id);
    }

    Ok(())
}
