/// Estimate the token count for a text string.
///
/// Uses a simple heuristic of ~4 characters per token, which is a reasonable
/// approximation for English text with the BGE tokenizer.
pub fn estimate_tokens(text: &str) -> usize {
    // ~4 chars per token is a well-known heuristic for English text.
    // This slightly over-estimates, which is safe for budget enforcement.
    text.len().div_ceil(4)
}

/// Estimate token counts for a batch of texts.
pub fn estimate_tokens_batch(texts: &[&str]) -> Vec<usize> {
    texts.iter().map(|t| estimate_tokens(t)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_string() {
        assert_eq!(estimate_tokens(""), 0);
    }

    #[test]
    fn test_short_text() {
        // "hello" = 5 chars -> ceil(5/4) = 2 tokens
        assert_eq!(estimate_tokens("hello"), 2);
    }

    #[test]
    fn test_longer_text() {
        let text = "The quick brown fox jumps over the lazy dog.";
        let tokens = estimate_tokens(text);
        // 44 chars -> 11 tokens
        assert_eq!(tokens, 11);
    }

    #[test]
    fn test_batch() {
        let texts = vec!["hello", "world", ""];
        let results = estimate_tokens_batch(&texts);
        assert_eq!(results, vec![2, 2, 0]);
    }

    #[test]
    fn test_never_exceeds_char_count() {
        let text = "a";
        assert!(estimate_tokens(text) <= text.len());
    }
}
