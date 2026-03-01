#!/usr/bin/env bash
set -euo pipefail

level="${1:-}"
if [[ ! "$level" =~ ^(patch|minor|major)$ ]]; then
  echo "Usage: $0 <patch|minor|major>"
  exit 1
fi

# Bump workspace version in all Cargo.toml files
cargo set-version --workspace --bump "$level"

# Read the new version
version=$(cargo metadata --no-deps --format-version=1 | jq -r '.packages[0].version')
tag="v${version}"

echo "Bumped to ${tag}"

# Regenerate changelog with the new tag
git-cliff --tag "$tag" -o CHANGELOG.md

# Commit and tag
git add --all
git commit -m "chore(release): ${tag}"
git tag "$tag"

echo "Tagged ${tag} — push with: git push origin master ${tag}"
