use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{BrainCoreError, Result};

// ---------------------------------------------------------------------------
// Global config (~/.brain/config.toml)
// ---------------------------------------------------------------------------

/// Top-level global configuration stored at `$BRAIN_HOME/config.toml`.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct GlobalConfig {
    /// Default model directory (optional override).
    pub model_dir: Option<PathBuf>,
    /// Default log level.
    pub log_level: Option<String>,
    /// Registered brains keyed by name.
    #[serde(default)]
    pub brains: HashMap<String, BrainEntry>,
}

/// An entry for a registered brain inside the global config.
#[derive(Debug, Serialize, Deserialize)]
pub struct BrainEntry {
    /// Absolute path to the project root.
    pub root: PathBuf,
    /// Note directory paths (absolute).
    #[serde(default)]
    pub notes: Vec<PathBuf>,
}

// ---------------------------------------------------------------------------
// Per-project config (.brain/brain.toml)
// ---------------------------------------------------------------------------

/// Project-local configuration stored at `<project>/.brain/brain.toml`.
#[derive(Debug, Serialize, Deserialize)]
pub struct BrainToml {
    /// Human-readable brain name.
    pub name: String,
    /// Relative paths to note directories.
    #[serde(default)]
    pub notes: Vec<PathBuf>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Return the brain home directory (`$BRAIN_HOME` or `~/.brain`).
pub fn brain_home() -> Result<PathBuf> {
    if let Ok(val) = std::env::var("BRAIN_HOME") {
        return Ok(PathBuf::from(val));
    }
    dirs::home_dir()
        .map(|h| h.join(".brain"))
        .ok_or_else(|| BrainCoreError::Config("cannot determine home directory".into()))
}

/// Load the global config, returning a default if the file does not exist.
pub fn load_global_config() -> Result<GlobalConfig> {
    let path = brain_home()?.join("config.toml");
    if !path.exists() {
        return Ok(GlobalConfig::default());
    }
    let text = std::fs::read_to_string(&path).map_err(BrainCoreError::Io)?;
    toml::from_str(&text)
        .map_err(|e| BrainCoreError::Config(format!("failed to parse {}: {e}", path.display())))
}

/// Persist the global config to disk, creating parent directories as needed.
pub fn save_global_config(cfg: &GlobalConfig) -> Result<()> {
    let home = brain_home()?;
    std::fs::create_dir_all(&home).map_err(BrainCoreError::Io)?;
    let path = home.join("config.toml");
    let text = toml::to_string_pretty(cfg)
        .map_err(|e| BrainCoreError::Config(format!("failed to serialize config: {e}")))?;
    std::fs::write(&path, text).map_err(BrainCoreError::Io)?;
    Ok(())
}

/// Load a project-local `brain.toml`.
pub fn load_brain_toml(brain_dir: &Path) -> Result<BrainToml> {
    let path = brain_dir.join("brain.toml");
    let text = std::fs::read_to_string(&path).map_err(BrainCoreError::Io)?;
    toml::from_str(&text)
        .map_err(|e| BrainCoreError::Config(format!("failed to parse {}: {e}", path.display())))
}

/// Write a project-local `brain.toml`.
pub fn save_brain_toml(brain_dir: &Path, cfg: &BrainToml) -> Result<()> {
    std::fs::create_dir_all(brain_dir).map_err(BrainCoreError::Io)?;
    let path = brain_dir.join("brain.toml");
    let text = toml::to_string_pretty(cfg)
        .map_err(|e| BrainCoreError::Config(format!("failed to serialize brain.toml: {e}")))?;
    std::fs::write(&path, text).map_err(BrainCoreError::Io)?;
    Ok(())
}
