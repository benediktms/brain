use std::sync::Arc;

use crate::error::BrainCoreError;

/// Backend-agnostic summarization trait.
///
/// Implementors: `MockSummarizer` (tests), pluggable backends via the job queue.
pub trait Summarize: Send + Sync {
    /// Summarize a single text input.
    fn summarize(&self, text: &str) -> crate::error::Result<String>;

    /// Batch summarization with default serial impl.
    /// Backends can override for batched inference.
    fn summarize_batch(&self, texts: &[&str]) -> crate::error::Result<Vec<String>> {
        texts.iter().map(|t| self.summarize(t)).collect()
    }

    /// Human-readable backend name for audit columns.
    fn backend_name(&self) -> &'static str;
}

/// Async wrapper for `Summarize::summarize` — runs on `spawn_blocking` to
/// avoid blocking the Tokio runtime with CPU-intensive summarization work.
pub async fn summarize_async(
    summarizer: &Arc<dyn Summarize>,
    text: String,
) -> crate::error::Result<String> {
    let summarizer = Arc::clone(summarizer);
    tokio::task::spawn_blocking(move || summarizer.summarize(&text))
        .await
        .map_err(|e| BrainCoreError::Embedding(format!("spawn_blocking: {e}")))?
}

/// A deterministic mock summarizer for tests.
/// Returns a fixed prefix with the first 50 characters of input.
pub struct MockSummarizer;

impl Summarize for MockSummarizer {
    fn summarize(&self, text: &str) -> crate::error::Result<String> {
        Ok(format!("Summary of: {}", &text[..text.len().min(50)]))
    }

    fn backend_name(&self) -> &'static str {
        "mock"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mock_summarizer_returns_expected_output() {
        let summarizer = MockSummarizer;
        let result = summarizer.summarize("hello world").unwrap();
        assert_eq!(result, "Summary of: hello world");
    }

    #[test]
    fn mock_summarizer_truncates_at_50_chars() {
        let summarizer = MockSummarizer;
        let long_text = "a".repeat(100);
        let result = summarizer.summarize(&long_text).unwrap();
        assert_eq!(result, format!("Summary of: {}", "a".repeat(50)));
    }

    #[test]
    fn mock_summarizer_backend_name() {
        let summarizer = MockSummarizer;
        assert_eq!(summarizer.backend_name(), "mock");
    }

    #[test]
    fn mock_summarizer_batch() {
        let summarizer = MockSummarizer;
        let texts = ["hello", "world"];
        let results = summarizer.summarize_batch(&texts).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], "Summary of: hello");
        assert_eq!(results[1], "Summary of: world");
    }
}
