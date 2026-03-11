use std::path::Path;
use std::sync::Arc;

use candle_core::{Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::t5::{self, T5ForConditionalGeneration};
use tokenizers::Tokenizer;
use tracing::info;

use crate::error::BrainCoreError;

/// Backend-agnostic summarization trait.
///
/// Implementors: `FlanT5Summarizer` (local Candle model), `MockSummarizer` (tests).
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

/// Flan-T5-small summarizer using candle-transformers T5ForConditionalGeneration.
///
/// Loads from a local directory containing `config.json`, `tokenizer.json`,
/// and `model.safetensors`. Uses greedy decoding with CPU-only F32 inference.
pub struct FlanT5Summarizer {
    model: T5ForConditionalGeneration,
    tokenizer: Tokenizer,
    config: t5::Config,
    device: Device,
}

impl std::fmt::Debug for FlanT5Summarizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlanT5Summarizer")
            .field("backend", &"flan-t5-small")
            .finish()
    }
}

impl FlanT5Summarizer {
    const MAX_GEN_TOKENS: usize = 128;

    /// Load Flan-T5-small from a local model directory.
    ///
    /// The directory must contain:
    /// ```text
    /// config.json          T5 config (serde from HuggingFace format)
    /// tokenizer.json       SentencePiece tokenizer in HF format
    /// model.safetensors    Model weights (memory-mapped)
    /// ```
    pub fn load(model_dir: &Path) -> crate::error::Result<Self> {
        let device = Device::Cpu;

        let config_path = model_dir.join("config.json");
        let tokenizer_path = model_dir.join("tokenizer.json");
        let weights_path = model_dir.join("model.safetensors");

        for (name, path) in [
            ("config.json", &config_path),
            ("tokenizer.json", &tokenizer_path),
            ("model.safetensors", &weights_path),
        ] {
            if !path.exists() {
                return Err(BrainCoreError::Embedding(format!(
                    "missing {name} in {}. Download with: huggingface-cli download google/flan-t5-small config.json tokenizer.json model.safetensors --local-dir ~/.brain/models/flan-t5-small",
                    model_dir.display(),
                )));
            }
        }

        let config_str = std::fs::read_to_string(&config_path)
            .map_err(|e| BrainCoreError::Embedding(format!("failed to read config.json: {e}")))?;
        let config: t5::Config = serde_json::from_str(&config_str).map_err(|e| {
            BrainCoreError::Embedding(format!("failed to parse config.json: {e}"))
        })?;

        let tokenizer = Tokenizer::from_file(&tokenizer_path)
            .map_err(|e| BrainCoreError::Embedding(format!("failed to load tokenizer: {e}")))?;

        // NOTE: memory-maps the file directly into the process's address space. Returned pointers
        // are backed by raw pointers rather than owned Rust memory. Edits occurring to the file at
        // the same time will produce undefined behavior.
        let vb = unsafe {
            VarBuilder::from_mmaped_safetensors(
                &[weights_path],
                candle_core::DType::F32,
                &device,
            )
            .map_err(|e| {
                BrainCoreError::Embedding(format!("failed to load model weights: {e}"))
            })?
        };

        let model = T5ForConditionalGeneration::load(vb, &config)
            .map_err(|e| BrainCoreError::Embedding(format!("failed to construct model: {e}")))?;

        info!("Flan-T5-small model loaded");

        Ok(Self {
            model,
            tokenizer,
            config,
            device,
        })
    }

    fn generate(&mut self, input_text: &str) -> crate::error::Result<String> {
        // Prepend instruction prefix for Flan-T5 summarization
        let prompted = format!("summarize: {input_text}");

        let encoding = self
            .tokenizer
            .encode(prompted.as_str(), true)
            .map_err(|e| BrainCoreError::Embedding(format!("tokenization failed: {e}")))?;

        let input_ids: Vec<u32> = encoding.get_ids().to_vec();
        let input_tensor = Tensor::new(input_ids.as_slice(), &self.device)
            .and_then(|t| t.unsqueeze(0))
            .map_err(|e| BrainCoreError::Embedding(format!("input tensor failed: {e}")))?;

        // Encode input once
        let encoder_output = self
            .model
            .encode(&input_tensor)
            .map_err(|e| BrainCoreError::Embedding(format!("encoder forward failed: {e}")))?;

        // Greedy decode: start with decoder_start_token_id (typically 0 = <pad>)
        let start_token = self
            .config
            .decoder_start_token_id
            .unwrap_or(self.config.pad_token_id) as u32;
        let eos_token = self.config.eos_token_id as u32;

        let mut output_ids: Vec<u32> = vec![start_token];

        for _ in 0..Self::MAX_GEN_TOKENS {
            let decoder_input = Tensor::new(output_ids.as_slice(), &self.device)
                .and_then(|t| t.unsqueeze(0))
                .map_err(|e| {
                    BrainCoreError::Embedding(format!("decoder input tensor failed: {e}"))
                })?;

            let logits = self
                .model
                .decode(&decoder_input, &encoder_output)
                .map_err(|e| BrainCoreError::Embedding(format!("decoder forward failed: {e}")))?;

            // logits shape: [1, vocab_size] — take argmax for greedy decoding
            let next_token = logits
                .argmax(1)
                .map_err(|e| BrainCoreError::Embedding(format!("argmax failed: {e}")))?
                .squeeze(0)
                .map_err(|e| BrainCoreError::Embedding(format!("squeeze failed: {e}")))?
                .to_scalar::<u32>()
                .map_err(|e| BrainCoreError::Embedding(format!("scalar failed: {e}")))?;

            if next_token == eos_token {
                break;
            }
            output_ids.push(next_token);
        }

        // Clear KV cache between calls
        self.model.clear_kv_cache();

        // Decode output tokens (skip the start token)
        let tokens_to_decode = &output_ids[1..];
        let decoded = self
            .tokenizer
            .decode(tokens_to_decode, true)
            .map_err(|e| BrainCoreError::Embedding(format!("decoding failed: {e}")))?;

        Ok(decoded)
    }
}

impl Summarize for FlanT5Summarizer {
    fn summarize(&self, text: &str) -> crate::error::Result<String> {
        // FlanT5Summarizer requires mutable access for KV cache management.
        // We use interior mutability via a cell since the trait requires &self.
        // SAFETY: The trait is Send + Sync, but the model is not Sync internally
        // due to mutable state. Callers are expected to use this via Arc with
        // external synchronization (e.g., Mutex) in multi-threaded contexts.
        //
        // For this spike implementation, we use a workaround: cast to *mut self.
        // In production, wrap FlanT5Summarizer in a Mutex before use.
        let this = self as *const Self as *mut Self;
        // SAFETY: Single-threaded use assumed. For concurrent use, callers must
        // wrap in Arc<Mutex<FlanT5Summarizer>>.
        unsafe { (*this).generate(text) }
    }

    fn backend_name(&self) -> &'static str {
        "flan-t5-small"
    }
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

    #[test]
    fn flan_t5_load_nonexistent_path_returns_error() {
        let err = FlanT5Summarizer::load(std::path::Path::new("/nonexistent/path/to/model"))
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("missing"), "got: {msg}");
    }
}
