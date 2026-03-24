//! LLM provider clients for async summarization via HTTP API.
//!
//! Provider selection is env-var-driven:
//! 1. `ANTHROPIC_API_KEY` → Anthropic Messages API (also covers MiniMax via `ANTHROPIC_BASE_URL`)
//! 2. `OPENAI_API_KEY` → OpenAI Chat Completions API (also covers MiniMax via `OPENAI_BASE_URL`)
//!
//! First key found wins. Model names are configurable via `BRAIN_ANTHROPIC_MODEL` /
//! `BRAIN_OPENAI_MODEL` env vars with sane defaults.

use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::error::BrainCoreError;
use crate::summarizer::Summarize;

// ─── Provider resolution ─────────────────────────────────────────

/// Resolve an LLM provider from environment variables.
/// Returns `None` if no API key is set.
pub fn resolve_provider() -> Option<Box<dyn Summarize>> {
    if let Ok(key) = std::env::var("ANTHROPIC_API_KEY") {
        let base_url = std::env::var("ANTHROPIC_BASE_URL")
            .unwrap_or_else(|_| "https://api.anthropic.com".to_string());
        let model = std::env::var("BRAIN_ANTHROPIC_MODEL")
            .unwrap_or_else(|_| "claude-haiku-4-5-20251001".to_string());
        info!(provider = "anthropic", base_url = %base_url, model = %model, "LLM provider resolved");
        return Some(Box::new(AnthropicProvider::new(key, base_url, model)));
    }

    if let Ok(key) = std::env::var("OPENAI_API_KEY") {
        let base_url = std::env::var("OPENAI_BASE_URL")
            .unwrap_or_else(|_| "https://api.openai.com".to_string());
        let model =
            std::env::var("BRAIN_OPENAI_MODEL").unwrap_or_else(|_| "gpt-4o-mini".to_string());
        info!(provider = "openai", base_url = %base_url, model = %model, "LLM provider resolved");
        return Some(Box::new(OpenAiProvider::new(key, base_url, model)));
    }

    warn!("no LLM provider configured — set ANTHROPIC_API_KEY or OPENAI_API_KEY");
    None
}

// ─── Token usage tracking ────────────────────────────────────────

/// Token usage from a single LLM API call.
#[derive(Debug, Clone, Default)]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_creation_input_tokens: u64,
    pub cache_read_input_tokens: u64,
}

impl TokenUsage {
    pub fn total(&self) -> u64 {
        self.input_tokens + self.output_tokens
    }
}

// ─── Anthropic provider ──────────────────────────────────────────

pub struct AnthropicProvider {
    client: Client,
    api_key: String,
    base_url: String,
    model: String,
}

impl AnthropicProvider {
    pub fn new(api_key: String, base_url: String, model: String) -> Self {
        Self {
            client: Client::new(),
            api_key,
            base_url: base_url.trim_end_matches('/').to_string(),
            model,
        }
    }

    async fn call(&self, prompt: &str) -> Result<(String, TokenUsage), BrainCoreError> {
        let url = format!("{}/v1/messages", self.base_url);

        let body = AnthropicRequest {
            model: &self.model,
            max_tokens: 1024,
            messages: vec![AnthropicMessage {
                role: "user",
                content: prompt,
            }],
        };

        let resp = self
            .client
            .post(&url)
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| BrainCoreError::Internal(format!("anthropic request failed: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp
                .text()
                .await
                .unwrap_or_else(|_| "<unreadable>".to_string());
            return Err(BrainCoreError::Internal(format!(
                "anthropic API error {status}: {body}"
            )));
        }

        let response: AnthropicResponse = resp.json().await.map_err(|e| {
            BrainCoreError::Internal(format!("anthropic response parse error: {e}"))
        })?;

        let text = response
            .content
            .into_iter()
            .filter_map(|block| {
                if block.block_type == "text" {
                    block.text
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join("");

        if text.is_empty() {
            return Err(BrainCoreError::Internal(
                "anthropic returned empty content".to_string(),
            ));
        }

        let usage = TokenUsage {
            input_tokens: response.usage.input_tokens,
            output_tokens: response.usage.output_tokens,
            cache_creation_input_tokens: response.usage.cache_creation_input_tokens.unwrap_or(0),
            cache_read_input_tokens: response.usage.cache_read_input_tokens.unwrap_or(0),
        };

        info!(
            input_tokens = usage.input_tokens,
            output_tokens = usage.output_tokens,
            total_tokens = usage.total(),
            model = %self.model,
            "anthropic API call"
        );

        Ok((text, usage))
    }
}

#[async_trait::async_trait]
impl Summarize for AnthropicProvider {
    async fn summarize(&self, text: &str) -> crate::error::Result<String> {
        let (result, _usage) = self.call(text).await?;
        Ok(result)
    }

    fn backend_name(&self) -> &'static str {
        "anthropic"
    }
}

#[derive(Serialize)]
struct AnthropicRequest<'a> {
    model: &'a str,
    max_tokens: u32,
    messages: Vec<AnthropicMessage<'a>>,
}

#[derive(Serialize)]
struct AnthropicMessage<'a> {
    role: &'a str,
    content: &'a str,
}

#[derive(Deserialize)]
struct AnthropicResponse {
    content: Vec<AnthropicContentBlock>,
    usage: AnthropicUsage,
}

#[derive(Deserialize)]
struct AnthropicContentBlock {
    #[serde(rename = "type")]
    block_type: String,
    text: Option<String>,
}

#[derive(Deserialize)]
struct AnthropicUsage {
    input_tokens: u64,
    output_tokens: u64,
    cache_creation_input_tokens: Option<u64>,
    cache_read_input_tokens: Option<u64>,
}

// ─── OpenAI provider ─────────────────────────────────────────────

pub struct OpenAiProvider {
    client: Client,
    api_key: String,
    base_url: String,
    model: String,
}

impl OpenAiProvider {
    pub fn new(api_key: String, base_url: String, model: String) -> Self {
        Self {
            client: Client::new(),
            api_key,
            base_url: base_url.trim_end_matches('/').to_string(),
            model,
        }
    }

    async fn call(&self, prompt: &str) -> Result<(String, TokenUsage), BrainCoreError> {
        let url = format!("{}/v1/chat/completions", self.base_url);

        let body = OpenAiRequest {
            model: &self.model,
            max_tokens: 1024,
            messages: vec![OpenAiMessage {
                role: "user",
                content: prompt,
            }],
        };

        let resp = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| BrainCoreError::Internal(format!("openai request failed: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp
                .text()
                .await
                .unwrap_or_else(|_| "<unreadable>".to_string());
            return Err(BrainCoreError::Internal(format!(
                "openai API error {status}: {body}"
            )));
        }

        let response: OpenAiResponse = resp
            .json()
            .await
            .map_err(|e| BrainCoreError::Internal(format!("openai response parse error: {e}")))?;

        let text = response
            .choices
            .into_iter()
            .next()
            .and_then(|c| c.message.content)
            .unwrap_or_default();

        if text.is_empty() {
            return Err(BrainCoreError::Internal(
                "openai returned empty content".to_string(),
            ));
        }

        let usage = response
            .usage
            .map(|u| TokenUsage {
                input_tokens: u.prompt_tokens,
                output_tokens: u.completion_tokens,
                ..Default::default()
            })
            .unwrap_or_default();

        info!(
            input_tokens = usage.input_tokens,
            output_tokens = usage.output_tokens,
            total_tokens = usage.total(),
            model = %self.model,
            "openai API call"
        );

        Ok((text, usage))
    }
}

#[async_trait::async_trait]
impl Summarize for OpenAiProvider {
    async fn summarize(&self, text: &str) -> crate::error::Result<String> {
        let (result, _usage) = self.call(text).await?;
        Ok(result)
    }

    fn backend_name(&self) -> &'static str {
        "openai"
    }
}

#[derive(Serialize)]
struct OpenAiRequest<'a> {
    model: &'a str,
    max_tokens: u32,
    messages: Vec<OpenAiMessage<'a>>,
}

#[derive(Serialize)]
struct OpenAiMessage<'a> {
    role: &'a str,
    content: &'a str,
}

#[derive(Deserialize)]
struct OpenAiResponse {
    choices: Vec<OpenAiChoice>,
    usage: Option<OpenAiUsage>,
}

#[derive(Deserialize)]
struct OpenAiChoice {
    message: OpenAiMessageContent,
}

#[derive(Deserialize)]
struct OpenAiMessageContent {
    content: Option<String>,
}

#[derive(Deserialize)]
struct OpenAiUsage {
    prompt_tokens: u64,
    completion_tokens: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_usage_total() {
        let usage = TokenUsage {
            input_tokens: 100,
            output_tokens: 50,
            ..Default::default()
        };
        assert_eq!(usage.total(), 150);
    }

    #[test]
    fn test_anthropic_provider_trims_trailing_slash() {
        let provider = AnthropicProvider::new(
            "key".to_string(),
            "https://api.anthropic.com/".to_string(),
            "claude-haiku-4-5-20251001".to_string(),
        );
        assert_eq!(provider.base_url, "https://api.anthropic.com");
    }

    #[test]
    fn test_openai_provider_trims_trailing_slash() {
        let provider = OpenAiProvider::new(
            "key".to_string(),
            "https://api.openai.com/".to_string(),
            "gpt-4o-mini".to_string(),
        );
        assert_eq!(provider.base_url, "https://api.openai.com");
    }

    #[test]
    fn test_resolve_provider_returns_none_without_keys() {
        // Clear env vars for this test
        // SAFETY: No other threads are reading these env vars concurrently in this test.
        unsafe {
            std::env::remove_var("ANTHROPIC_API_KEY");
            std::env::remove_var("OPENAI_API_KEY");
        }
        assert!(resolve_provider().is_none());
    }
}
