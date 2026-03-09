use std::path::Path;

use rusqlite::{Connection, OptionalExtension};
use tracing::warn;

use crate::error::Result;

/// Get a meta value by key.
pub fn get_meta(conn: &Connection, key: &str) -> Result<Option<String>> {
    let result = conn
        .query_row(
            "SELECT value FROM brain_meta WHERE key = ?1",
            [key],
            |row| row.get(0),
        )
        .optional()?;
    Ok(result)
}

/// Get a meta value by key, parsed as u32.
///
/// Returns `Ok(None)` if the key does not exist or the stored value is not a
/// valid u32. Emits a warning on parse failure so bad values are visible in logs.
pub fn get_meta_u32(conn: &Connection, key: &str) -> Result<Option<u32>> {
    match get_meta(conn, key)? {
        None => Ok(None),
        Some(s) => match s.parse::<u32>() {
            Ok(v) => Ok(Some(v)),
            Err(_) => {
                warn!(key = key, value = %s, "brain_meta value is not a valid u32, treating as unset");
                Ok(None)
            }
        },
    }
}

/// Set a meta value (upsert).
pub fn set_meta(conn: &Connection, key: &str, value: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO brain_meta (key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        rusqlite::params![key, value],
    )?;
    Ok(())
}

/// Get the project prefix. Auto-generates from directory name on first call.
///
/// The prefix is stored in `brain_meta` and cached after first generation.
/// If a stored prefix is invalid (e.g. manually corrupted), it is regenerated.
///
/// Note: within a single process, the `Db` Mutex serializes access, so the
/// read-then-write is safe. Cross-process races on a fresh database could
/// theoretically produce two different prefixes, but `INSERT ... ON CONFLICT`
/// ensures the second writer's value wins deterministically.
pub fn get_or_init_project_prefix(conn: &Connection, brain_dir: &Path) -> Result<String> {
    if let Some(prefix) = get_meta(conn, "project_prefix")? {
        // Validate stored prefix — reject corrupted values
        if prefix.len() == 3 && prefix.chars().all(|c| c.is_ascii_uppercase()) {
            return Ok(prefix);
        }
        // Invalid stored prefix, fall through to regenerate
    }

    // Derive from the brain data directory name (e.g. ~/.brain/brains/<name>/)
    let name = brain_dir
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("BRN");

    let prefix = generate_prefix(name);
    set_meta(conn, "project_prefix", &prefix)?;
    Ok(prefix)
}

/// Generate a 3-letter project prefix from a name.
///
/// Algorithm:
/// 1. Split on `-`, `_`, spaces
/// 2. Drop pure-numeric segments ("02", "v3") — keep mixed like "b2c"
/// 3. Multi-word (>= 3 segments): first letter of first 3 non-duplicate segments
/// 4. Two segments: first letter of each + first consonant of longer segment
/// 5. Single word: first char + next consonants (skip AEIOU)
/// 6. Uppercase, pad to 3 chars if needed
pub fn generate_prefix(name: &str) -> String {
    let segments: Vec<&str> = name
        .split(['-', '_', ' '])
        .filter(|s| !s.is_empty())
        .filter(|s| !is_pure_numeric(s))
        .collect();

    let raw = match segments.len() {
        0 => "BRN".to_string(),
        1 => prefix_from_single_word(segments[0]),
        2 => prefix_from_two_words(segments[0], segments[1]),
        _ => prefix_from_multi_words(&segments),
    };

    // Ensure exactly 3 uppercase ASCII letters
    let upper = raw.to_ascii_uppercase();
    let chars: Vec<char> = upper.chars().filter(|c| c.is_ascii_alphabetic()).collect();

    match chars.len() {
        0 => "BRN".to_string(),
        1 => format!("{}XX", chars[0]),
        2 => format!("{}{}X", chars[0], chars[1]),
        _ => chars[..3].iter().collect(),
    }
}

/// Check if a segment is purely numeric (e.g. "02", "123").
fn is_pure_numeric(s: &str) -> bool {
    s.chars().all(|c| c.is_ascii_digit())
}

/// Check if a char is a vowel.
fn is_vowel(c: char) -> bool {
    matches!(c.to_ascii_uppercase(), 'A' | 'E' | 'I' | 'O' | 'U')
}

/// Extract consonants from a word (skipping the first character).
fn consonants_after_first(word: &str) -> Vec<char> {
    word.chars()
        .skip(1)
        .filter(|c| c.is_ascii_alphabetic() && !is_vowel(*c))
        .collect()
}

/// Single word: first char + next two consonants.
///
/// Fallback: if fewer than 2 consonants exist, takes remaining alphabetic
/// characters (preferring unique ones) until we have 3 chars.
fn prefix_from_single_word(word: &str) -> String {
    let first = word.chars().next().unwrap_or('X');
    let consonants = consonants_after_first(word);
    let mut result = vec![first];
    for c in consonants {
        if result.len() >= 3 {
            break;
        }
        result.push(c);
    }
    // If we still need chars, take remaining unique alphabetic chars
    if result.len() < 3 {
        for c in word.chars().skip(1).filter(|c| c.is_ascii_alphabetic()) {
            if !result.iter().any(|r| r.eq_ignore_ascii_case(&c)) || result.len() < 3 {
                result.push(c);
            }
            if result.len() >= 3 {
                break;
            }
        }
    }
    result.into_iter().collect()
}

/// Two words: first letter of each + first consonant of the longer word.
fn prefix_from_two_words(a: &str, b: &str) -> String {
    let first_a = a.chars().next().unwrap_or('X');
    let first_b = b.chars().next().unwrap_or('X');
    let longer = if a.len() >= b.len() { a } else { b };
    let consonants = consonants_after_first(longer);
    let third = consonants.first().copied().unwrap_or_else(|| {
        // fallback: second char of longer word
        longer.chars().nth(1).unwrap_or('X')
    });
    format!("{}{}{}", first_a, first_b, third)
}

/// Multi-word (3+): first letter of first 3 segments, skipping duplicate letters.
fn prefix_from_multi_words(segments: &[&str]) -> String {
    let mut result = Vec::new();
    let mut seen = Vec::new();

    for seg in segments {
        if result.len() >= 3 {
            break;
        }
        if let Some(c) = seg.chars().next() {
            let upper = c.to_ascii_uppercase();
            if !seen.contains(&upper) {
                result.push(c);
                seen.push(upper);
            }
        }
    }

    // If we still need chars (due to duplicates), pull consonants from remaining segments
    if result.len() < 3 {
        for seg in segments {
            if result.len() >= 3 {
                break;
            }
            for c in consonants_after_first(seg) {
                let upper = c.to_ascii_uppercase();
                if !seen.contains(&upper) {
                    result.push(c);
                    seen.push(upper);
                    break;
                }
            }
        }
    }

    result.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema::init_schema;

    fn setup() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        conn
    }

    // -- generate_prefix tests --

    #[test]
    fn test_prefix_single_word() {
        assert_eq!(generate_prefix("brain"), "BRN");
    }

    #[test]
    fn test_prefix_single_word_short() {
        assert_eq!(generate_prefix("go"), "GOX");
    }

    #[test]
    fn test_prefix_drops_numeric_segments() {
        assert_eq!(generate_prefix("brain-02"), "BRN");
    }

    #[test]
    fn test_prefix_multi_word() {
        assert_eq!(generate_prefix("my-cool-project"), "MCP");
    }

    #[test]
    fn test_prefix_two_words() {
        assert_eq!(generate_prefix("auth-service"), "ASR");
    }

    #[test]
    fn test_prefix_multi_word_dedup() {
        // a-b-a would have duplicate A; skip second A, take next segment
        assert_eq!(generate_prefix("app-b2c-api-gateway"), "ABG");
    }

    #[test]
    fn test_prefix_empty() {
        assert_eq!(generate_prefix(""), "BRN");
    }

    #[test]
    fn test_prefix_all_numeric() {
        assert_eq!(generate_prefix("123-456"), "BRN");
    }

    #[test]
    fn test_prefix_underscores() {
        assert_eq!(generate_prefix("my_cool_project"), "MCP");
    }

    // -- meta storage tests --

    #[test]
    fn test_get_set_meta() {
        let conn = setup();
        assert_eq!(get_meta(&conn, "foo").unwrap(), None);

        set_meta(&conn, "foo", "bar").unwrap();
        assert_eq!(get_meta(&conn, "foo").unwrap(), Some("bar".to_string()));

        // Upsert
        set_meta(&conn, "foo", "baz").unwrap();
        assert_eq!(get_meta(&conn, "foo").unwrap(), Some("baz".to_string()));
    }

    #[test]
    fn test_get_or_init_project_prefix() {
        let conn = setup();
        // brain_dir is the data directory: ~/.brain/brains/<name>/
        let brain_dir = Path::new("/home/user/.brain/brains/brain-02");
        let prefix = get_or_init_project_prefix(&conn, brain_dir).unwrap();
        assert_eq!(prefix, "BRN");

        // Second call returns cached value
        let prefix2 = get_or_init_project_prefix(&conn, brain_dir).unwrap();
        assert_eq!(prefix, prefix2);
    }

    #[test]
    fn test_get_or_init_uses_data_dir_name() {
        let conn = setup();
        let brain_dir = Path::new("/home/user/.brain/brains/my-cool-project");
        let prefix = get_or_init_project_prefix(&conn, brain_dir).unwrap();
        assert_eq!(prefix, "MCP");
    }

    #[test]
    fn test_manual_override() {
        let conn = setup();
        let brain_dir = Path::new("/home/user/.brain/brains/whatever");

        // Auto-generate first
        let _ = get_or_init_project_prefix(&conn, brain_dir).unwrap();

        // Override
        set_meta(&conn, "project_prefix", "XYZ").unwrap();
        let prefix = get_or_init_project_prefix(&conn, brain_dir).unwrap();
        assert_eq!(prefix, "XYZ");
    }

    #[test]
    fn test_get_meta_u32() {
        let conn = setup();

        // Missing key → None
        assert_eq!(get_meta_u32(&conn, "no_such_key").unwrap(), None);

        // Valid u32
        set_meta(&conn, "version", "42").unwrap();
        assert_eq!(get_meta_u32(&conn, "version").unwrap(), Some(42));

        // Zero is valid
        set_meta(&conn, "version", "0").unwrap();
        assert_eq!(get_meta_u32(&conn, "version").unwrap(), Some(0));

        // Unparseable → None (warning emitted)
        set_meta(&conn, "version", "not_a_number").unwrap();
        assert_eq!(get_meta_u32(&conn, "version").unwrap(), None);

        // Negative number → None (u32 can't be negative)
        set_meta(&conn, "version", "-1").unwrap();
        assert_eq!(get_meta_u32(&conn, "version").unwrap(), None);
    }

    #[test]
    fn test_invalid_stored_prefix_is_regenerated() {
        let conn = setup();
        let brain_dir = Path::new("/home/user/.brain/brains/brain");

        // Store an invalid prefix (too short)
        set_meta(&conn, "project_prefix", "AB").unwrap();
        let prefix = get_or_init_project_prefix(&conn, brain_dir).unwrap();
        // Should regenerate from dir name, not return "AB"
        assert_eq!(prefix.len(), 3);
        assert!(prefix.chars().all(|c| c.is_ascii_uppercase()));
        assert_ne!(prefix, "AB");

        // Store an invalid prefix (lowercase)
        set_meta(&conn, "project_prefix", "abc").unwrap();
        let prefix = get_or_init_project_prefix(&conn, brain_dir).unwrap();
        assert_eq!(prefix.len(), 3);
        assert!(prefix.chars().all(|c| c.is_ascii_uppercase()));
    }
}
