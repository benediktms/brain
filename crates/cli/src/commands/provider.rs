use std::io::{self, BufRead, Write};

/// Resolve an API key — from argument or stdin prompt.
fn get_api_key(name: &str, api_key: Option<&str>) -> Result<String> {
    match api_key {
        Some(k) if !k.trim().is_empty() => Ok(k.trim().to_string()),
        Some(_) => anyhow::bail!("API key cannot be empty"),
        None => {
            eprint!("Enter API key for {name}: ");
            io::stderr().flush()?;
            let mut line = String::new();
            io::stdin().lock().read_line(&mut line)?;
            let trimmed = line.trim().to_string();
            if trimmed.is_empty() {
                anyhow::bail!("API key cannot be empty");
            }
            Ok(trimmed)
        }
    }
}
use std::path::Path;

use anyhow::Result;
use brain_lib::config::{brain_home, project_providers_to_config};
use brain_lib::ports::ProviderStore;
use brain_lib::providers::VALID_PROVIDERS;
use brain_lib::stores::BrainStores;
use brain_persistence::db::crypto;
use brain_persistence::db::providers::InsertProvider;

use crate::commands::rpc_client;

/// `brain config provider set <name> [api_key]`
pub fn run_set(
    sqlite_db: &Path,
    lance_db: Option<&Path>,
    name: &str,
    api_key: Option<&str>,
    remote: bool,
) -> Result<()> {
    // Validate provider name before prompting for key via stdin
    if !VALID_PROVIDERS.contains(&name) {
        anyhow::bail!(
            "invalid provider name '{}'. Valid options: {}",
            name,
            VALID_PROVIDERS.join(", ")
        );
    }

    if remote {
        let mut client = rpc_client::connect_daemon()?;
        let key = get_api_key(name, api_key)?;
        let id = client
            .provider_set(name, &key)
            .map_err(|e| anyhow::anyhow!("ProviderSet rpc failed: {e}"))?;
        println!("Provider '{name}' configured (id: {id})");
        return Ok(());
    }

    let key = get_api_key(name, api_key)?;

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

fn run_list_remote() -> Result<()> {
    let mut client = rpc_client::connect_daemon()?;
    let providers = client
        .provider_list()
        .map_err(|e| anyhow::anyhow!("ProviderList rpc failed: {e}"))?;

    if providers.is_empty() {
        println!("No providers configured.");
        println!("Run `brain config provider set <anthropic|openai> <api-key>` to add one.");
        return Ok(());
    }

    println!("Configured Providers");
    for p in &providers {
        println!("  {} — {} (key: {}…)", p.id, p.name, p.key_hash_prefix);
    }

    Ok(())
}

/// `brain config provider list`
pub fn run_list(sqlite_db: &Path, lance_db: Option<&Path>, remote: bool) -> Result<()> {
    if remote {
        return run_list_remote();
    }
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
pub fn run_remove(
    sqlite_db: &Path,
    lance_db: Option<&Path>,
    target: &str,
    remote: bool,
) -> Result<()> {
    if remote {
        let mut client = rpc_client::connect_daemon()?;
        client
            .provider_remove(target)
            .map_err(|e| anyhow::anyhow!("ProviderRemove rpc failed: {e}"))?;
        println!("Removed all providers named '{target}'");
        return Ok(());
    }

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
