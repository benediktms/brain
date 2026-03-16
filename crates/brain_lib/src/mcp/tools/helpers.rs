use serde::Serialize;
use serde_json::Value;

use crate::db::Db;
use crate::mcp::protocol::ToolCallResult;

/// Return `true` if the brain with `brain_id` has been archived.
///
/// Returns `false` when no matching row exists (brain not yet registered).
/// Returns `Err` only on a hard database failure.
pub fn is_brain_archived(db: &Db, brain_id: &str) -> Result<bool, String> {
    db.with_read_conn(|conn| {
        let mut stmt = conn
            .prepare_cached("SELECT archived FROM brains WHERE brain_id = ?1")
            .map_err(|e| crate::error::BrainCoreError::Database(format!("{e}")))?;
        let mut rows = stmt
            .query([brain_id])
            .map_err(|e| crate::error::BrainCoreError::Database(format!("{e}")))?;
        if let Some(row) = rows
            .next()
            .map_err(|e| crate::error::BrainCoreError::Database(format!("{e}")))?
        {
            let archived: i64 = row
                .get(0)
                .map_err(|e| crate::error::BrainCoreError::Database(format!("{e}")))?;
            Ok(archived == 1)
        } else {
            Ok(false)
        }
    })
    .map_err(|e| e.to_string())
}

#[derive(Serialize, Debug, Clone)]
pub struct Warning {
    pub source: String,
    pub error: String,
}

pub fn json_response(value: &impl Serialize) -> ToolCallResult {
    match serde_json::to_string_pretty(value) {
        Ok(json) => ToolCallResult::text(json),
        Err(err) => ToolCallResult::error(format!("Internal serialization error: {err}")),
    }
}

pub fn store_or_warn<T: Default>(
    result: Result<T, impl std::fmt::Display>,
    source: &str,
    warnings: &mut Vec<Warning>,
) -> T {
    match result {
        Ok(value) => value,
        Err(err) => {
            warnings.push(Warning {
                source: source.to_string(),
                error: err.to_string(),
            });
            T::default()
        }
    }
}

pub fn inject_warnings(response: &mut Value, warnings: Vec<Warning>) {
    if warnings.is_empty() {
        return;
    }

    if let Value::Object(map) = response
        && let Ok(warnings_value) = serde_json::to_value(warnings)
    {
        map.insert("warnings".to_string(), warnings_value);
    }
}

#[cfg(test)]
mod tests {
    use serde::ser::Error as _;
    use serde::ser::Serializer;
    use serde_json::json;

    use super::*;

    struct AlwaysFailSerialize;

    impl Serialize for AlwaysFailSerialize {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            Err(S::Error::custom("boom"))
        }
    }

    #[derive(Serialize)]
    struct ResponseWithWarnings {
        data: String,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        warnings: Vec<Warning>,
    }

    #[test]
    fn json_response_success() {
        let response = json_response(&json!({"ok": true, "count": 1}));
        assert_eq!(response.is_error, None);

        let text = &response.content[0].text;
        let parsed: Value = serde_json::from_str(text).expect("json response should be valid");
        assert_eq!(parsed["ok"], true);
        assert_eq!(parsed["count"], 1);
    }

    #[test]
    fn json_response_failure() {
        let response = json_response(&AlwaysFailSerialize);
        assert_eq!(response.is_error, Some(true));
        assert!(
            response.content[0]
                .text
                .to_lowercase()
                .contains("serialization error")
        );
    }

    #[test]
    fn store_or_warn_ok() {
        let mut warnings = Vec::new();
        let input: Result<Vec<&str>, &str> = Ok(vec!["a"]);
        let result = store_or_warn(input, "get_items", &mut warnings);

        assert_eq!(result, vec!["a"]);
        assert!(warnings.is_empty());
    }

    #[test]
    fn store_or_warn_err() {
        let mut warnings = Vec::new();
        let result: Vec<String> = store_or_warn(Err("db broken"), "get_task_labels", &mut warnings);

        assert!(result.is_empty());
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].source, "get_task_labels");
        assert_eq!(warnings[0].error, "db broken");
    }

    #[test]
    fn warnings_skip_when_empty() {
        let payload = ResponseWithWarnings {
            data: "ok".into(),
            warnings: vec![],
        };

        let value = serde_json::to_value(payload).expect("serializes");
        let object = value.as_object().expect("object");

        assert_eq!(object.get("data"), Some(&json!("ok")));
        assert!(!object.contains_key("warnings"));
    }
}
