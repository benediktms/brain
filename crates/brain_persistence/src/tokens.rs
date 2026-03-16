/// Estimate the token count for a text string.
///
/// Uses a simple heuristic of ~4 characters per token, which is a reasonable
/// approximation for English text with the BGE tokenizer.
pub fn estimate_tokens(text: &str) -> usize {
    text.len().div_ceil(4)
}
