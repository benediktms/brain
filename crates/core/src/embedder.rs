use std::path::{Path, PathBuf};

use candle_core::{Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::bert::{BertModel, Config};
use hf_hub::api::sync::Api;
use tokenizers::Tokenizer;
use tracing::info;

use crate::error::BrainCoreError;

const HF_REPO_ID: &str = "BAAI/bge-small-en-v1.5";

pub struct Embedder {
    model: BertModel,
    tokenizer: Tokenizer,
    device: Device,
    hidden_size: usize,
}

impl Embedder {
    /// Resolve model file paths — use a local directory if provided, otherwise
    /// download from HuggingFace Hub (cached at `~/.cache/huggingface/hub/`).
    fn resolve_paths(
        model_dir: Option<&Path>,
    ) -> crate::error::Result<(PathBuf, PathBuf, PathBuf)> {
        if let Some(dir) = model_dir {
            let config = dir.join("config.json");
            let tokenizer = dir.join("tokenizer.json");
            let weights = dir.join("model.safetensors");

            for (name, path) in [
                ("config.json", &config),
                ("tokenizer.json", &tokenizer),
                ("model.safetensors", &weights),
            ] {
                if !path.exists() {
                    return Err(BrainCoreError::Embedding(format!(
                        "missing {name} in {}",
                        dir.display(),
                    )));
                }
            }

            Ok((config, tokenizer, weights))
        } else {
            info!("downloading model {HF_REPO_ID} from HuggingFace Hub (cached after first run)");

            let api = Api::new().map_err(|e| {
                BrainCoreError::Embedding(format!("failed to create HF Hub client: {e}"))
            })?;
            let repo = api.model(HF_REPO_ID.to_string());

            let config = repo.get("config.json").map_err(|e| {
                BrainCoreError::Embedding(format!("failed to fetch config.json: {e}"))
            })?;
            let tokenizer = repo.get("tokenizer.json").map_err(|e| {
                BrainCoreError::Embedding(format!("failed to fetch tokenizer.json: {e}"))
            })?;
            let weights = repo.get("model.safetensors").map_err(|e| {
                BrainCoreError::Embedding(format!("failed to fetch model.safetensors: {e}"))
            })?;

            info!("model files ready");
            Ok((config, tokenizer, weights))
        }
    }

    /// Load BGE-small-en-v1.5.
    ///
    /// If `model_dir` is `Some`, reads from that local directory.
    /// If `None`, downloads from HuggingFace Hub (cached for subsequent runs).
    pub fn load(model_dir: Option<&Path>) -> crate::error::Result<Self> {
        let device = Device::Cpu;
        let (config_path, tokenizer_path, weights_path) = Self::resolve_paths(model_dir)?;

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

        let tokenizer = Tokenizer::from_file(&tokenizer_path)
            .map_err(|e| BrainCoreError::Embedding(format!("failed to load tokenizer: {e}")))?;

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

    /// Embed a batch of text strings, returning 384-dim L2-normalized vectors.
    pub fn embed_batch(&self, texts: &[&str]) -> crate::error::Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(vec![]);
        }

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
