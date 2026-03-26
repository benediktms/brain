//! Backend-agnostic summarization trait and implementations.

/// Backend-agnostic summarization trait (async).
///
/// Implementors: `MockSummarizer` (tests), `AnthropicProvider` / `OpenAiProvider` (HTTP API clients in `llm` module).
#[async_trait::async_trait]
pub trait Summarize: Send + Sync {
    /// Summarize a single text input.
    async fn summarize(&self, text: &str) -> crate::error::Result<String>;

    /// Batch summarization with default serial impl.
    /// Backends can override for batched inference.
    async fn summarize_batch(&self, texts: &[&str]) -> crate::error::Result<Vec<String>> {
        let mut results = Vec::with_capacity(texts.len());
        for t in texts {
            results.push(self.summarize(t).await?);
        }
        Ok(results)
    }

    /// Human-readable backend name for audit columns.
    fn backend_name(&self) -> &'static str;
}

/// A deterministic mock summarizer for tests.
/// Returns a fixed prefix with the first 50 characters of input.
pub struct MockSummarizer;

#[async_trait::async_trait]
impl Summarize for MockSummarizer {
    async fn summarize(&self, text: &str) -> crate::error::Result<String> {
        Ok(format!("Summary of: {}", &text[..text.len().min(50)]))
    }

    fn backend_name(&self) -> &'static str {
        "mock"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mock_summarizer_returns_expected_output() {
        let summarizer = MockSummarizer;
        let result = summarizer.summarize("hello world").await.unwrap();
        assert_eq!(result, "Summary of: hello world");
    }

    #[tokio::test]
    async fn mock_summarizer_truncates_at_50_chars() {
        let summarizer = MockSummarizer;
        let long_text = "a".repeat(100);
        let result = summarizer.summarize(&long_text).await.unwrap();
        assert_eq!(result, format!("Summary of: {}", "a".repeat(50)));
    }

    #[test]
    fn mock_summarizer_backend_name() {
        let summarizer = MockSummarizer;
        assert_eq!(summarizer.backend_name(), "mock");
    }

    #[tokio::test]
    async fn mock_summarizer_batch() {
        let summarizer = MockSummarizer;
        let texts = ["hello", "world"];
        let results = summarizer.summarize_batch(&texts).await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], "Summary of: hello");
        assert_eq!(results[1], "Summary of: world");
    }
}
