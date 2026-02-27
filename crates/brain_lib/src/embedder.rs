use std::path::Path;

use candle_core::{Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::bert::{BertModel, Config};
use tokenizers::Tokenizer;
use tracing::info;

use crate::error::BrainCoreError;

/// Trait for embedding text into vectors.
pub trait Embed: Send + Sync {
    fn embed_batch(&self, texts: &[&str]) -> crate::error::Result<Vec<Vec<f32>>>;
    fn hidden_size(&self) -> usize;
}

pub struct Embedder {
    model: BertModel,
    tokenizer: Tokenizer,
    device: Device,
    hidden_size: usize,
}

impl Embedder {
    /// Load BGE-small-en-v1.5 from a local directory.
    ///
    /// The directory must contain `config.json`, `tokenizer.json`, and
    /// `model.safetensors`. Use `scripts/setup-model.sh` to download them.
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
                    "missing {name} in {} — run scripts/setup-model.sh to download",
                    model_dir.display(),
                )));
            }
        }

        let config_str = std::fs::read_to_string(&config_path)
            .map_err(|e| BrainCoreError::Embedding(format!("failed to read config.json: {e}")))?;
        let config: Config = serde_json::from_str(&config_str)
            .map_err(|e| BrainCoreError::Embedding(format!("failed to parse config.json: {e}")))?;

        let hidden_size = config.hidden_size;
        if hidden_size != 384 {
            return Err(BrainCoreError::Embedding(format!(
                "expected hidden_size=384, got {hidden_size}"
            )));
        }

        let mut tokenizer = Tokenizer::from_file(&tokenizer_path)
            .map_err(|e| BrainCoreError::Embedding(format!("failed to load tokenizer: {e}")))?;

        // Enable padding so encode_batch produces equal-length sequences for batched inference.
        let pad_id = tokenizer.token_to_id("[PAD]").unwrap_or(0);
        tokenizer.with_padding(Some(tokenizers::PaddingParams {
            pad_id,
            pad_token: "[PAD]".to_string(),
            ..Default::default()
        }));

        // NOTE: memory-maps the file directly into the process's address space. Returned pointers
        // are backed by raw pointers rather than owned Rust memory. Edits occuring to the file at
        // the same time will produce undefined behavior.
        let vb = unsafe {
            VarBuilder::from_mmaped_safetensors(&[weights_path], candle_core::DType::F32, &device)
                .map_err(|e| BrainCoreError::Embedding(format!("failed to load model weights: {e}")))?
        };

        let model = BertModel::load(vb, &config)
            .map_err(|e| BrainCoreError::Embedding(format!("failed to construct model: {e}")))?;

        info!(hidden_size, "BGE-small model loaded");

        Ok(Self {
            model,
            tokenizer,
            device,
            hidden_size,
        })
    }

    const MAX_BATCH_SIZE: usize = 32;

    /// Embed a batch of text strings, returning 384-dim L2-normalized vectors.
    /// Internally processes in sub-batches of [`Self::MAX_BATCH_SIZE`] to bound
    /// memory usage and keep debug-build latency manageable.
    pub fn embed_batch(&self, texts: &[&str]) -> crate::error::Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(vec![]);
        }

        let mut all_embeddings = Vec::with_capacity(texts.len());
        for chunk in texts.chunks(Self::MAX_BATCH_SIZE) {
            let batch_result = self.embed_batch_inner(chunk)?;
            all_embeddings.extend(batch_result);
        }
        Ok(all_embeddings)
    }

    /// Run a single sub-batch through the model.
    fn embed_batch_inner(&self, texts: &[&str]) -> crate::error::Result<Vec<Vec<f32>>> {
        let encodings = self
            .tokenizer
            .encode_batch(texts.to_vec(), true)
            .map_err(|e| BrainCoreError::Embedding(format!("tokenization failed: {e}")))?;

        let token_ids: Vec<Tensor> = encodings
            .iter()
            .map(|enc| {
                let ids: Vec<u32> = enc.get_ids().to_vec();
                Tensor::new(ids.as_slice(), &self.device).map(|t| t.unsqueeze(0))
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| BrainCoreError::Embedding(format!("tensor creation failed: {e}")))?
            .into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| BrainCoreError::Embedding(format!("unsqueeze failed: {e}")))?;

        let attention_masks: Vec<Tensor> = encodings
            .iter()
            .map(|enc| {
                let mask: Vec<u32> = enc.get_attention_mask().to_vec();
                Tensor::new(mask.as_slice(), &self.device).map(|t| t.unsqueeze(0))
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| BrainCoreError::Embedding(format!("attention mask failed: {e}")))?
            .into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| BrainCoreError::Embedding(format!("unsqueeze failed: {e}")))?;

        let token_ids = Tensor::cat(&token_ids, 0)
            .map_err(|e| BrainCoreError::Embedding(format!("token cat failed: {e}")))?;
        let attention_mask = Tensor::cat(&attention_masks, 0)
            .map_err(|e| BrainCoreError::Embedding(format!("mask cat failed: {e}")))?;
        let token_type_ids = token_ids
            .zeros_like()
            .map_err(|e| BrainCoreError::Embedding(format!("token_type_ids failed: {e}")))?;

        // Forward pass: [B, T] -> [B, T, H]
        let hidden_states = self
            .model
            .forward(&token_ids, &token_type_ids, Some(&attention_mask))
            .map_err(|e| BrainCoreError::Embedding(format!("forward pass failed: {e}")))?;

        // CLS pooling: take first token -> [B, H]
        let cls = hidden_states
            .narrow(1, 0, 1)
            .map_err(|e| BrainCoreError::Embedding(format!("CLS select failed: {e}")))?
            .squeeze(1)
            .map_err(|e| BrainCoreError::Embedding(format!("squeeze failed: {e}")))?;

        // L2 normalize with epsilon clamp for numerical stability
        let l2_norm = cls
            .sqr()
            .and_then(|s| s.sum_keepdim(1))
            .and_then(|s| s.sqrt())
            .and_then(|s| s.clamp(1e-12, f32::MAX as f64))
            .map_err(|e| BrainCoreError::Embedding(format!("L2 norm failed: {e}")))?;

        let normalized = cls
            .broadcast_div(&l2_norm)
            .map_err(|e| BrainCoreError::Embedding(format!("normalization failed: {e}")))?;

        let result = normalized
            .to_vec2::<f32>()
            .map_err(|e| BrainCoreError::Embedding(format!("to_vec2 failed: {e}")))?;

        // Debug assertion: verify all vectors are unit length
        debug_assert!(result.iter().all(|v| {
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            (1.0 - norm).abs() < 1e-5
        }));

        Ok(result)
    }

    pub fn hidden_size(&self) -> usize {
        self.hidden_size
    }
}

impl Embed for Embedder {
    fn embed_batch(&self, texts: &[&str]) -> crate::error::Result<Vec<Vec<f32>>> {
        self.embed_batch(texts)
    }

    fn hidden_size(&self) -> usize {
        self.hidden_size
    }
}

/// A mock embedder that produces deterministic hash-based 384-dim vectors.
/// Avoids requiring model weights in CI/tests.
pub struct MockEmbedder;

impl Embed for MockEmbedder {
    fn embed_batch(&self, texts: &[&str]) -> crate::error::Result<Vec<Vec<f32>>> {
        Ok(texts.iter().map(|text| mock_embedding(text)).collect())
    }

    fn hidden_size(&self) -> usize {
        384
    }
}

/// Generate a deterministic 384-dim unit vector from text content using BLAKE3.
fn mock_embedding(text: &str) -> Vec<f32> {
    let hash = blake3::hash(text.as_bytes());
    let bytes = hash.as_bytes();

    let mut embedding = Vec::with_capacity(384);
    for i in 0..384 {
        // Use hash bytes cyclically to fill 384 dimensions
        let byte = bytes[i % 32];
        embedding.push((byte as f32 / 255.0) - 0.5);
    }

    // L2 normalize
    let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for v in &mut embedding {
            *v /= norm;
        }
    }

    embedding
}
