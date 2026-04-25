use std::io::{self, BufRead, Write};
use std::path::Path;

use anyhow::Result;
use brain_lib::config::{ProviderEntry, brain_home, load_global_config, save_global_config};
use brain_lib::ports::ProviderStore;
use brain_lib::stores::BrainStores;
use brain_persistence::db::crypto;
use brain_persistence::db::providers::InsertProvider;

const VALID_PROVIDERS: &[&str] = &["anthropic", "openai"];

/// `brain config provider set <name> [api_key]`
pub fn run_set(
    sqlite_db: &Path,
    lance_db: Option<&Path>,
    name: &str,
    api_key: Option<&str>,
) -> Result<()> {
    // Validate provider name
    if !VALID_PROVIDERS.contains(&name) {
        anyhow::bail!(
            "invalid provider name '{}'. Valid options: {}",
            name,
            VALID_PROVIDERS.join(", ")
        );
    }

    // Get or prompt for the API key
    let key = match api_key {
        Some(k) => k.to_string(),
        None => {
            eprint!("Enter API key for {name}: ");
            io::stderr().flush()?;
            let mut line = String::new();
            io::stdin().lock().read_line(&mut line)?;
            let trimmed = line.trim().to_string();
            if trimmed.is_empty() {
                anyhow::bail!("API key cannot be empty");
            }
            trimmed
        }
    };

    let home = brain_home()?;
    let master_key = crypto::load_or_create_master_key(&home)?;

    // Check for duplicate
    let key_hash = crypto::hash_api_key(&key);
    let stores = BrainStores::from_path(sqlite_db, lance_db)?;

    if stores.provider_exists(name, &key_hash)? {
        println!("Provider '{name}' with this key already exists.");
        return Ok(());
    }

    // Encrypt and store
    let encrypted = crypto::encrypt(&master_key, &key)?;
    let id = stores.insert_provider(&InsertProvider {
        name,
        api_key_encrypted: &encrypted,
        api_key_hash: &key_hash,
    })?;

    // Project to state_projection.toml
    project_providers_to_config(&stores)?;

    println!("Provider '{name}' configured (id: {id})");
    Ok(())
}

/// `brain config provider list`
pub fn run_list(sqlite_db: &Path, lance_db: Option<&Path>) -> Result<()> {
    let stores = BrainStores::from_path(sqlite_db, lance_db)?;
    let providers = stores.list_providers()?;

    if providers.is_empty() {
        println!("No providers configured.");
        println!("Run `brain config provider set <anthropic|openai> <api-key>` to add one.");
        return Ok(());
    }

    println!("Configured Providers");
    for p in &providers {
        let masked_hash = &p.api_key_hash[..8];
        println!("  {} — {} (key: {}…)", p.id, p.name, masked_hash);
    }

    Ok(())
}

/// `brain config provider remove <target>`
pub fn run_remove(sqlite_db: &Path, lance_db: Option<&Path>, target: &str) -> Result<()> {
    let stores = BrainStores::from_path(sqlite_db, lance_db)?;

    // Try by ID first
    if stores.delete_provider(target)? {
        project_providers_to_config(&stores)?;
        println!("Removed provider {target}");
        return Ok(());
    }

    // Try by name — delete all entries for that provider name
    let providers = stores.list_providers()?;
    let matching: Vec<_> = providers.iter().filter(|p| p.name == target).collect();

    if matching.is_empty() {
        anyhow::bail!("no provider found matching '{target}'");
    }

    for p in &matching {
        stores.delete_provider(&p.id)?;
    }

    project_providers_to_config(&stores)?;
    println!(
        "Removed {} provider{} for '{target}'",
        matching.len(),
        if matching.len() == 1 { "" } else { "s" }
    );

    Ok(())
}

/// Sync the providers list from DB to state_projection.toml (metadata only).
fn project_providers_to_config(stores: &BrainStores) -> Result<()> {
    let providers = stores.list_providers()?;
    let entries: Vec<ProviderEntry> = providers
        .iter()
        .map(|p| ProviderEntry {
            id: p.id.clone(),
            name: p.name.clone(),
        })
        .collect();

    let mut cfg = load_global_config()?;
    cfg.providers = entries;
    save_global_config(&cfg)?;

    Ok(())
}
