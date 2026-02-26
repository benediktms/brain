#!/usr/bin/env bash
set -euo pipefail

MODEL_REPO="BAAI/bge-small-en-v1.5"
MODEL_DIR="$(cd "$(dirname "$0")/.." && pwd)/models/bge-small-en-v1.5"

echo "==> Setting up BGE-small-en-v1.5 model"

# Check if model is already downloaded
if [[ -f "$MODEL_DIR/config.json" && -f "$MODEL_DIR/tokenizer.json" && -f "$MODEL_DIR/model.safetensors" ]]; then
    echo "Model already downloaded at $MODEL_DIR"
    exit 0
fi

# Resolve the HF CLI command (brew installs as 'hf', pipx as 'huggingface-cli')
HF_CMD=""
if command -v hf &>/dev/null; then
    HF_CMD="hf"
elif command -v huggingface-cli &>/dev/null; then
    HF_CMD="huggingface-cli"
fi

# Install if not available
if [[ -z "$HF_CMD" ]]; then
    echo "==> HuggingFace CLI not found, installing..."
    if [[ "$(uname -s)" == "Darwin" ]]; then
        if ! command -v brew &>/dev/null; then
            echo "Error: homebrew is required on macOS. Install from https://brew.sh"
            exit 1
        fi
        brew install huggingface-cli
        HF_CMD="hf"
    else
        # Linux: fall back to pipx
        if ! command -v pipx &>/dev/null; then
            echo "Error: pipx is required on Linux. Install with: sudo apt install pipx"
            exit 1
        fi
        pipx install huggingface-hub
        HF_CMD="huggingface-cli"
    fi
fi

# Download model
echo "==> Downloading $MODEL_REPO to $MODEL_DIR"
mkdir -p "$MODEL_DIR"
"$HF_CMD" download "$MODEL_REPO" \
    config.json tokenizer.json model.safetensors \
    --local-dir "$MODEL_DIR"

echo "==> Model ready at $MODEL_DIR"
