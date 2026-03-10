use anyhow::Result;
use brain_lib::mcp::tools::ToolRegistry;

/// Output the JSON schema for all MCP tool definitions.
pub fn run(tool: Option<String>, pretty: bool) -> Result<()> {
    let registry = ToolRegistry::new();
    let definitions = registry.definitions();

    let output = if let Some(name) = tool {
        // Map dotted MCP name (e.g. "tasks.apply_event") and underscore CLI name
        // (e.g. "tasks_apply_event") so either form works.
        let matching: Vec<_> = definitions
            .into_iter()
            .filter(|d| d.name == name || d.name.replace('.', "_") == name)
            .collect();

        if matching.is_empty() {
            anyhow::bail!("No tool found matching '{name}'");
        }

        if pretty {
            serde_json::to_string_pretty(&matching)?
        } else {
            serde_json::to_string(&matching)?
        }
    } else if pretty {
        serde_json::to_string_pretty(&definitions)?
    } else {
        serde_json::to_string(&definitions)?
    };

    println!("{output}");
    Ok(())
}
