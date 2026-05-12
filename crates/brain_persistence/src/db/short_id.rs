//! Shared short-ID helpers used by both tasks and sagas.
//!
//! Both domains derive a compact display ID from their canonical record ID
//! (a ULID) via BLAKE3, then truncate to the shortest unique prefix. Tasks
//! scope uniqueness per-brain (`UNIQUE(brain_id, display_id)`); sagas scope
//! it globally (`UNIQUE(display_id)`). The hashing and collision-extension
//! algorithm is identical; this module is the single source of truth.

use std::collections::HashSet;

/// Minimum length for the hex portion of a hash-based short ID.
pub const MIN_SHORT_HASH_LEN: usize = 3;

/// BLAKE3 hash of `input` → full 64-char lowercase hex string.
///
/// Pure function, no DB access. Used by migration backfills and projection
/// insert paths to derive a deterministic display-ID seed.
pub fn blake3_short_hex(input: &str) -> String {
    blake3::hash(input.as_bytes()).to_hex().to_string()
}

/// Pick the shortest unique prefix of `full_hex` not already present in `used`.
///
/// Starts at `MIN_SHORT_HASH_LEN` and extends one char at a time until a free
/// slot is found. Returns `full_hex` itself if all positions are exhausted
/// (extremely unlikely with 64 hex chars). The caller is responsible for
/// enforcing the appropriate UNIQUE constraint on the chosen prefix.
pub fn pick_unique_prefix(full_hex: &str, used: &HashSet<String>) -> String {
    for len in MIN_SHORT_HASH_LEN..=full_hex.len() {
        let candidate = &full_hex[..len];
        if !used.contains(candidate) {
            return candidate.to_string();
        }
    }
    full_hex.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blake3_short_hex_is_deterministic() {
        let a = blake3_short_hex("hello");
        let b = blake3_short_hex("hello");
        assert_eq!(a, b);
        assert_eq!(a.len(), 64);
        assert!(
            a.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }

    #[test]
    fn pick_unique_prefix_starts_at_min_len() {
        let hex = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let used = HashSet::new();
        assert_eq!(pick_unique_prefix(hex, &used), "abc");
    }

    #[test]
    fn pick_unique_prefix_extends_on_collision() {
        let hex = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let mut used = HashSet::new();
        used.insert("abc".to_string());
        assert_eq!(pick_unique_prefix(hex, &used), "abcd");
    }

    #[test]
    fn pick_unique_prefix_extends_multiple_steps() {
        let hex = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let mut used = HashSet::new();
        used.insert("abc".to_string());
        used.insert("abcd".to_string());
        used.insert("abcde".to_string());
        assert_eq!(pick_unique_prefix(hex, &used), "abcdef");
    }
}
