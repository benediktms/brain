use std::io::Read;
use std::path::Path;

use candle_core::{Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::bert::{BertModel, Config};
use tokenizers::Tokenizer;
use tracing::info;

use crate::error::BrainCoreError;

/// User-facing instructions for downloading the embedding model.
pub const MODEL_DOWNLOAD_HINT: &str = "\
To download the model, either run the setup script:\n\
  curl -sSL https://raw.githubusercontent.com/benediktms/brain/master/scripts/setup-model.sh | bash\n\
Or install the HuggingFace CLI manually:\n\
  pip install huggingface_hub\n\
  hf download BAAI/bge-small-en-v1.5 config.json tokenizer.json model.safetensors \
--local-dir ~/.brain/models/bge-small-en-v1.5";

/// Expected BLAKE3 checksums for BGE-small-en-v1.5 model artifacts.
/// Computed against the pinned HuggingFace revision of BAAI/bge-small-en-v1.5.
const EXPECTED_CHECKSUMS: &[(&str, &str)] = &[
    (
        "config.json",
        "a2dadabd189ddc5bffa8db91d3ee1b0872c35b09db2488efab23ae5c1d93cd60",
    ),
    (
        "tokenizer.json",
        "6e933bf59db40b8b2a0de480fe5006662770757e1e1671eb7e48ff6a5f00b0b4",
    ),
    (
        "model.safetensors",
        "6588b38fa23ad13648a2678bc8cd8733bf4be79ba12ac6dfa1368d33d80e8fc7",
    ),
];

/// Verify BLAKE3 checksums of model artifacts against expected values.
///
/// Uses streaming reads (64KB buffer) to avoid loading the full ~130MB
/// safetensors file into memory. Returns `Ok(())` if all checksums match,
/// or an `Embedding` error with the first mismatch details.
fn verify_checksums(model_dir: &Path, expected: &[(&str, &str)]) -> crate::error::Result<()> {
    for &(filename, expected_hex) in expected {
        let path = model_dir.join(filename);
        let mut file = std::fs::File::open(&path).map_err(|e| {
            BrainCoreError::Embedding(format!(
                "failed to read {filename} for checksum verification: {e}"
            ))
        })?;

        let mut hasher = blake3::Hasher::new();
        let mut buf = [0u8; 65536];
        loop {
            let n = file
                .read(&mut buf)
                .map_err(|e| BrainCoreError::Embedding(format!("error reading {filename}: {e}")))?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }

        let actual_hex = hasher.finalize().to_hex().to_string();
        if actual_hex != expected_hex {
            return Err(BrainCoreError::Embedding(format!(
                "checksum mismatch for {filename}: expected {expected_hex}, got {actual_hex}. \
                 Delete {} and re-download the model.\n{MODEL_DOWNLOAD_HINT}",
                model_dir.display(),
            )));
        }
    }
    Ok(())
}

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
    /// The directory must contain the following artifacts:
    ///
    /// ```text
    /// ~/.brain/models/bge-small-en-v1.5/
    ///   config.json          BERT config (hidden_size=384)
    ///   tokenizer.json       WordPiece tokenizer
    ///   model.safetensors    Model weights (~130MB, memory-mapped)
    /// ```
    ///
    /// All files are verified against hardcoded BLAKE3 checksums before
    /// loading. The `model.safetensors` file is memory-mapped via `unsafe`
    /// into the process address space, so integrity verification is critical.
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
                    "missing {name} in {}.\n{MODEL_DOWNLOAD_HINT}",
                    model_dir.display(),
                )));
            }
        }

        verify_checksums(model_dir, EXPECTED_CHECKSUMS)?;
        info!("model checksums verified");

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

        // Truncate to the model's max positional embedding size (512 tokens).
        // Without this, chunks exceeding 512 tokens crash with
        // "index-select invalid index 512 with dim size 512".
        tokenizer.with_truncation(Some(tokenizers::TruncationParams {
            max_length: 512,
            ..Default::default()
        })).map_err(|e| BrainCoreError::Embedding(format!("failed to set truncation: {e}")))?;

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

        let total = texts.len();
        let num_batches = total.div_ceil(Self::MAX_BATCH_SIZE);
        let mut all_embeddings = Vec::with_capacity(total);
        for (i, chunk) in texts.chunks(Self::MAX_BATCH_SIZE).enumerate() {
            info!(
                batch = i + 1,
                of = num_batches,
                size = chunk.len(),
                done = all_embeddings.len(),
                total,
                "embedding sub-batch"
            );
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

/// Async wrapper for `Embed::embed_batch` — runs on `spawn_blocking` to
/// avoid blocking the Tokio runtime with CPU-intensive embedding work.
pub async fn embed_batch_async(
    embedder: &std::sync::Arc<dyn Embed>,
    texts: Vec<String>,
) -> crate::error::Result<Vec<Vec<f32>>> {
    let embedder = std::sync::Arc::clone(embedder);
    tokio::task::spawn_blocking(move || {
        let refs: Vec<&str> = texts.iter().map(|s| s.as_str()).collect();
        embedder.embed_batch(&refs)
    })
    .await
    .map_err(|e| crate::error::BrainCoreError::Embedding(format!("spawn_blocking: {e}")))?
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn verify_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let expected = &[("config.json", "abcd1234")];
        let err = verify_checksums(dir.path(), expected).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("failed to read"), "got: {msg}");
        assert!(msg.contains("config.json"), "got: {msg}");
    }

    #[test]
    fn verify_wrong_checksum() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("config.json"), b"hello").unwrap();
        let expected = &[(
            "config.json",
            "0000000000000000000000000000000000000000000000000000000000000000",
        )];
        let err = verify_checksums(dir.path(), expected).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("checksum mismatch"), "got: {msg}");
        assert!(msg.contains("config.json"), "got: {msg}");
    }

    #[test]
    fn verify_correct_checksums() {
        let dir = tempfile::tempdir().unwrap();
        let content = b"test content for checksum";
        fs::write(dir.path().join("data.bin"), content).unwrap();

        let expected_hash = blake3::hash(content).to_hex().to_string();
        let expected = vec![("data.bin", expected_hash.as_str())];
        verify_checksums(dir.path(), &expected).unwrap();
    }
}
