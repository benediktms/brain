use std::path::Path;

use anyhow::{Result, anyhow};
use serde_json::json;

use brain_lib::sagas::SagaStore;
use brain_lib::stores::BrainStores;

pub struct SagaCtx {
    pub(crate) store: SagaStore,
    pub(crate) json: bool,
}

impl SagaCtx {
    pub fn new(sqlite_db: &Path, json: bool) -> Result<Self> {
        let stores = BrainStores::from_path(sqlite_db, None)?;
        Ok(Self {
            store: stores.sagas,
            json,
        })
    }
}

pub fn create(ctx: &SagaCtx, title: &str, description: Option<&str>) -> Result<()> {
    let row = ctx.store.create(title, description, "cli")?;
    if ctx.json {
        let out = json!({
            "saga_id": row.saga_id,
            "saga": {
                "saga_id": row.saga_id,
                "title": row.title,
                "description": row.description,
                "status": row.status,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
                "closed_at": row.closed_at,
                "members": [],
            }
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Created saga {}", row.saga_id);
        println!("  Title:  {}", row.title);
        println!("  Status: {}", row.status);
        if let Some(desc) = &row.description {
            println!("  Desc:   {desc}");
        }
    }
    Ok(())
}

pub fn show(ctx: &SagaCtx, saga_id: &str) -> Result<()> {
    let row = ctx
        .store
        .get(saga_id)?
        .ok_or_else(|| anyhow!("saga not found: {saga_id}"))?;

    if ctx.json {
        let out = json!({
            "saga_id": row.saga_id,
            "saga": {
                "saga_id": row.saga_id,
                "title": row.title,
                "description": row.description,
                "status": row.status,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
                "closed_at": row.closed_at,
                "members": [],
            }
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Saga {}", row.saga_id);
        println!("  Title:  {}", row.title);
        println!("  Status: {}", row.status);
        if let Some(desc) = &row.description {
            println!("  Desc:   {desc}");
        }
        if let Some(ts) = row.closed_at {
            println!("  Closed: {ts}");
        }
    }
    Ok(())
}
