#!/usr/bin/env bash
set -euo pipefail

branch=$(git rev-parse --abbrev-ref HEAD)
if [[ "$branch" != "master" ]]; then
  echo "Error: releases must be created from the master branch (currently on '$branch')"
  exit 1
fi

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
echo "This will trigger the release workflow (binary builds + Homebrew tap update)"
