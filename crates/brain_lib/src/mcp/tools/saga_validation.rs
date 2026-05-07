//! Shared validation helpers for saga MCP tool boundaries.
//!
//! All saga tools call these at the top of `execute()` before touching the
//! store. Failures surface as `ToolCallResult::error(...)` strings.

pub const MAX_TASKS_PER_BATCH: usize = 500;
pub const MAX_TITLE_LEN: usize = 1024;
pub const MAX_DESCRIPTION_LEN: usize = 64 * 1024;
pub const MAX_ACTOR_LEN: usize = 64;

/// Validate an actor string: 1–64 ASCII chars, alphanumeric + `_`, `-`, `:`.
pub fn validate_actor(s: &str) -> Result<&str, String> {
    if s.is_empty() {
        return Err("actor must not be empty".into());
    }
    if s.len() > MAX_ACTOR_LEN {
        return Err(format!(
            "actor exceeds maximum length of {MAX_ACTOR_LEN} characters"
        ));
    }
    if !s
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == ':')
    {
        return Err(
            "actor must contain only ASCII alphanumeric characters or `_`, `-`, `:`".into(),
        );
    }
    Ok(s)
}

/// Validate a saga ID: exactly 26 ASCII alphanumeric characters (Crockford-base32 ULID).
pub fn validate_saga_id(s: &str) -> Result<&str, String> {
    if s.len() != 26 {
        return Err(format!(
            "saga_id must be exactly 26 characters, got {}",
            s.len()
        ));
    }
    if !s.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Err("saga_id must contain only ASCII alphanumeric characters".into());
    }
    Ok(s)
}

/// Validate a single task ID: non-empty, at most 128 characters.
pub fn validate_task_id(s: &str) -> Result<&str, String> {
    if s.is_empty() {
        return Err("task_id must not be empty".into());
    }
    if s.len() > 128 {
        // Use a char-safe truncation; `&s[..32]` panics when the byte boundary
        // lands inside a multibyte UTF-8 character (e.g. a Japanese string).
        let preview: String = s.chars().take(32).collect();
        return Err(format!(
            "task_id exceeds maximum length of 128 characters: {preview}"
        ));
    }
    Ok(s)
}

/// Validate a saga title: non-empty after trimming, at most `MAX_TITLE_LEN` characters.
pub fn validate_title(s: &str) -> Result<&str, String> {
    if s.trim().is_empty() {
        return Err("title must not be empty".into());
    }
    if s.len() > MAX_TITLE_LEN {
        return Err(format!(
            "title exceeds maximum length of {MAX_TITLE_LEN} characters"
        ));
    }
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_task_id_truncates_long_multibyte_input_without_panic() {
        // 200 Japanese characters (each 3 bytes in UTF-8 -> 600 bytes total).
        let s: String = "あ".repeat(200);
        let result = validate_task_id(&s);
        assert!(result.is_err(), "should error on overlong input, not panic");
        let msg = result.unwrap_err();
        assert!(msg.contains("exceeds maximum length"));
    }
}

/// Validate an optional description: `None` is allowed; if `Some`, at most `MAX_DESCRIPTION_LEN` bytes.
pub fn validate_description(s: Option<&str>) -> Result<Option<&str>, String> {
    match s {
        None => Ok(None),
        Some(v) => {
            if v.len() > MAX_DESCRIPTION_LEN {
                return Err(format!(
                    "description exceeds maximum length of {MAX_DESCRIPTION_LEN} bytes"
                ));
            }
            Ok(Some(v))
        }
    }
}
