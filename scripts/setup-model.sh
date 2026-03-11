#!/usr/bin/env bash
set -euo pipefail

BRAIN_HOME="${BRAIN_HOME:-$HOME/.brain}"

# ── Model definitions ─────────────────────────────────────────────────────
# Each model: repo, local dir override env var, default subdir, required files
declare -A MODELS=(
    [bge-small]="BAAI/bge-small-en-v1.5"
    [flan-t5-small]="google/flan-t5-small"
)

declare -A MODEL_ENV=(
    [bge-small]="BRAIN_MODEL_DIR"
    [flan-t5-small]="BRAIN_SUMMARIZER_MODEL_DIR"
)

declare -A MODEL_DIRS=(
    [bge-small]="$BRAIN_HOME/models/bge-small-en-v1.5"
    [flan-t5-small]="$BRAIN_HOME/models/flan-t5-small"
)

MODEL_FILES="config.json tokenizer.json model.safetensors"

# ── HuggingFace CLI resolution ────────────────────────────────────────────
resolve_hf_cli() {
    if command -v hf &>/dev/null; then
        echo "hf"
    elif command -v huggingface-cli &>/dev/null; then
        echo "huggingface-cli"
    else
        echo ""
    fi
}

install_hf_cli() {
    echo "==> HuggingFace CLI not found, installing..."
    if [[ "$(uname -s)" == "Darwin" ]]; then
        if ! command -v brew &>/dev/null; then
            echo "Error: homebrew is required on macOS. Install from https://brew.sh"
            exit 1
        fi
        brew install huggingface-cli
        echo "hf"
    else
        if ! command -v pipx &>/dev/null; then
            echo "Error: pipx is required on Linux. Install with: sudo apt install pipx"
            exit 1
        fi
        pipx install huggingface-hub
        echo "huggingface-cli"
    fi
}

# ── Download a single model ───────────────────────────────────────────────
download_model() {
    local name="$1"
    local repo="${MODELS[$name]}"
    local env_var="${MODEL_ENV[$name]}"
    local default_dir="${MODEL_DIRS[$name]}"
    local model_dir="${!env_var:-$default_dir}"

    echo "==> Setting up $name ($repo)"

    # Check if already downloaded
    local all_present=true
    for f in $MODEL_FILES; do
        if [[ ! -f "$model_dir/$f" ]]; then
            all_present=false
            break
        fi
    done

    if [[ "$all_present" == "true" ]]; then
        echo "    Already downloaded at $model_dir"
        return 0
    fi

    # Download
    echo "    Downloading to $model_dir"
    mkdir -p "$model_dir"
    "$HF_CMD" download "$repo" $MODEL_FILES --local-dir "$model_dir"

    if command -v b3sum &>/dev/null; then
        echo "    BLAKE3 checksums:"
        for f in $MODEL_FILES; do
            b3sum "$model_dir/$f"
        done
    fi

    echo "    Ready at $model_dir"
}

# ── Main ──────────────────────────────────────────────────────────────────

# Parse args: specific models or all
REQUESTED=("$@")
if [[ ${#REQUESTED[@]} -eq 0 ]]; then
    REQUESTED=("bge-small" "flan-t5-small")
fi

# Resolve HF CLI
HF_CMD=$(resolve_hf_cli)
if [[ -z "$HF_CMD" ]]; then
    HF_CMD=$(install_hf_cli)
fi

for model in "${REQUESTED[@]}"; do
    if [[ -z "${MODELS[$model]+x}" ]]; then
        echo "Error: unknown model '$model'. Available: ${!MODELS[*]}"
        exit 1
    fi
    download_model "$model"
done

echo "==> All models ready."
