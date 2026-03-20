//! `brain://` URI scheme — parser, Display, and convenience constructors.
//!
//! URI format: `brain://<brain-name>/<domain>/<id>`

use std::fmt;
use std::str::FromStr;

use crate::error::BrainCoreError;

// ---------------------------------------------------------------------------
// Domain enum
// ---------------------------------------------------------------------------

/// The object domain encoded in a `brain://` URI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Domain {
    /// Records: artifacts and snapshots.
    Record,
    /// Tasks.
    Task,
    /// Memory chunks (note chunks).
    Memory,
    /// Episode summaries.
    Episode,
    /// Reflection summaries.
    Reflection,
    /// Procedure summaries.
    Procedure,
}

impl fmt::Display for Domain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Domain::Record => "record",
            Domain::Task => "task",
            Domain::Memory => "memory",
            Domain::Episode => "episode",
            Domain::Reflection => "reflection",
            Domain::Procedure => "procedure",
        };
        f.write_str(s)
    }
}

impl FromStr for Domain {
    type Err = BrainCoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "record" => Ok(Domain::Record),
            "task" => Ok(Domain::Task),
            "memory" => Ok(Domain::Memory),
            "episode" => Ok(Domain::Episode),
            "reflection" => Ok(Domain::Reflection),
            "procedure" => Ok(Domain::Procedure),
            _ => Err(BrainCoreError::Parse(format!("unknown domain: {s:?}"))),
        }
    }
}

// ---------------------------------------------------------------------------
// BrainUri struct
// ---------------------------------------------------------------------------

/// A parsed `brain://` URI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrainUri {
    /// Brain name (host segment of the URI).
    pub brain: String,
    /// Object domain.
    pub domain: Domain,
    /// Object ID.
    pub id: String,
}

impl BrainUri {
    /// Parse a `brain://` URI string.
    pub fn parse(s: &str) -> Result<Self, BrainCoreError> {
        s.parse()
    }

    // ------------------------------------------------------------------
    // Convenience constructors
    // ------------------------------------------------------------------

    pub fn for_record(brain: &str, id: &str) -> Self {
        Self {
            brain: brain.to_string(),
            domain: Domain::Record,
            id: id.to_string(),
        }
    }

    pub fn for_task(brain: &str, id: &str) -> Self {
        Self {
            brain: brain.to_string(),
            domain: Domain::Task,
            id: id.to_string(),
        }
    }

    pub fn for_memory(brain: &str, id: &str) -> Self {
        Self {
            brain: brain.to_string(),
            domain: Domain::Memory,
            id: id.to_string(),
        }
    }

    pub fn for_episode(brain: &str, id: &str) -> Self {
        Self {
            brain: brain.to_string(),
            domain: Domain::Episode,
            id: id.to_string(),
        }
    }

    pub fn for_reflection(brain: &str, id: &str) -> Self {
        Self {
            brain: brain.to_string(),
            domain: Domain::Reflection,
            id: id.to_string(),
        }
    }

    pub fn for_procedure(brain: &str, id: &str) -> Self {
        Self {
            brain: brain.to_string(),
            domain: Domain::Procedure,
            id: id.to_string(),
        }
    }
}

impl fmt::Display for BrainUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "brain://{}/{}/{}", self.brain, self.domain, self.id)
    }
}

impl FromStr for BrainUri {
    type Err = BrainCoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let rest = s
            .strip_prefix("brain://")
            .ok_or_else(|| BrainCoreError::Parse(format!("missing brain:// scheme: {s:?}")))?;

        // Split on '/' — expect exactly [brain, domain, id]
        let parts: Vec<&str> = rest.splitn(4, '/').collect();
        match parts.as_slice() {
            [brain, domain, id] => {
                if brain.is_empty() {
                    return Err(BrainCoreError::Parse("empty brain name".to_string()));
                }
                if id.is_empty() {
                    return Err(BrainCoreError::Parse("empty id".to_string()));
                }
                let domain = domain.parse::<Domain>()?;
                Ok(BrainUri {
                    brain: brain.to_string(),
                    domain,
                    id: id.to_string(),
                })
            }
            [brain, domain, id, _extra] => {
                // 4-part split means there was a 4th segment — too many
                let _ = (brain, domain, id);
                Err(BrainCoreError::Parse(
                    "too many path segments in brain:// URI".to_string(),
                ))
            }
            _ => Err(BrainCoreError::Parse(format!(
                "expected brain://<brain>/<domain>/<id>, got: {s:?}"
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// resolve_id helper
// ---------------------------------------------------------------------------

/// If the input is a `brain://` URI, extract the ID segment. Otherwise return as-is.
///
/// This is transparent: non-URI strings pass through unchanged, making it safe
/// to call on any ID parameter without breaking existing callers.
///
/// # Examples
///
/// ```
/// use brain_lib::uri::resolve_id;
///
/// assert_eq!(resolve_id("brain://my-proj/task/BRN-01ABC"), "BRN-01ABC");
/// assert_eq!(resolve_id("BRN-01ABC"), "BRN-01ABC");
/// assert_eq!(resolve_id("BRN-01"), "BRN-01"); // prefix — passed through
/// ```
pub fn resolve_id(input: &str) -> String {
    if let Ok(uri) = input.parse::<BrainUri>() {
        uri.id
    } else {
        input.to_string()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Domain Display / FromStr round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn domain_display_record() {
        assert_eq!(Domain::Record.to_string(), "record");
    }

    #[test]
    fn domain_display_task() {
        assert_eq!(Domain::Task.to_string(), "task");
    }

    #[test]
    fn domain_display_memory() {
        assert_eq!(Domain::Memory.to_string(), "memory");
    }

    #[test]
    fn domain_display_episode() {
        assert_eq!(Domain::Episode.to_string(), "episode");
    }

    #[test]
    fn domain_display_reflection() {
        assert_eq!(Domain::Reflection.to_string(), "reflection");
    }

    #[test]
    fn domain_display_procedure() {
        assert_eq!(Domain::Procedure.to_string(), "procedure");
    }

    #[test]
    fn domain_from_str_all_variants() {
        assert_eq!("record".parse::<Domain>().unwrap(), Domain::Record);
        assert_eq!("task".parse::<Domain>().unwrap(), Domain::Task);
        assert_eq!("memory".parse::<Domain>().unwrap(), Domain::Memory);
        assert_eq!("episode".parse::<Domain>().unwrap(), Domain::Episode);
        assert_eq!("reflection".parse::<Domain>().unwrap(), Domain::Reflection);
        assert_eq!("procedure".parse::<Domain>().unwrap(), Domain::Procedure);
    }

    #[test]
    fn domain_from_str_unknown_returns_error() {
        assert!("note".parse::<Domain>().is_err());
        assert!("chunk".parse::<Domain>().is_err());
        assert!("".parse::<Domain>().is_err());
        assert!("Record".parse::<Domain>().is_err()); // case-sensitive
    }

    #[test]
    fn domain_roundtrip() {
        for domain in [
            Domain::Record,
            Domain::Task,
            Domain::Memory,
            Domain::Episode,
            Domain::Reflection,
            Domain::Procedure,
        ] {
            let s = domain.to_string();
            let parsed: Domain = s.parse().unwrap();
            assert_eq!(parsed, domain);
        }
    }

    // -----------------------------------------------------------------------
    // BrainUri::parse — valid URIs (all 6 domains)
    // -----------------------------------------------------------------------

    #[test]
    fn parse_record_uri() {
        let uri = BrainUri::parse("brain://my-project/record/BRN-01ABC").unwrap();
        assert_eq!(uri.brain, "my-project");
        assert_eq!(uri.domain, Domain::Record);
        assert_eq!(uri.id, "BRN-01ABC");
    }

    #[test]
    fn parse_task_uri() {
        let uri = BrainUri::parse("brain://my-project/task/BRN-01DEF").unwrap();
        assert_eq!(uri.brain, "my-project");
        assert_eq!(uri.domain, Domain::Task);
        assert_eq!(uri.id, "BRN-01DEF");
    }

    #[test]
    fn parse_memory_uri() {
        let uri = BrainUri::parse("brain://my-project/memory/chunk-abc123").unwrap();
        assert_eq!(uri.brain, "my-project");
        assert_eq!(uri.domain, Domain::Memory);
        assert_eq!(uri.id, "chunk-abc123");
    }

    #[test]
    fn parse_episode_uri() {
        let uri = BrainUri::parse("brain://my-project/episode/01GHI").unwrap();
        assert_eq!(uri.brain, "my-project");
        assert_eq!(uri.domain, Domain::Episode);
        assert_eq!(uri.id, "01GHI");
    }

    #[test]
    fn parse_reflection_uri() {
        let uri = BrainUri::parse("brain://my-project/reflection/01JKL").unwrap();
        assert_eq!(uri.brain, "my-project");
        assert_eq!(uri.domain, Domain::Reflection);
        assert_eq!(uri.id, "01JKL");
    }

    #[test]
    fn parse_procedure_uri() {
        let uri = BrainUri::parse("brain://my-project/procedure/01MNO").unwrap();
        assert_eq!(uri.brain, "my-project");
        assert_eq!(uri.domain, Domain::Procedure);
        assert_eq!(uri.id, "01MNO");
    }

    #[test]
    fn parse_brain_name_with_hyphens_and_underscores() {
        let uri = BrainUri::parse("brain://my_brain-v2/task/TASK-001").unwrap();
        assert_eq!(uri.brain, "my_brain-v2");
        assert_eq!(uri.domain, Domain::Task);
        assert_eq!(uri.id, "TASK-001");
    }

    // -----------------------------------------------------------------------
    // BrainUri Display — round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn display_record_uri() {
        let uri = BrainUri::for_record("my-project", "BRN-01ABC");
        assert_eq!(uri.to_string(), "brain://my-project/record/BRN-01ABC");
    }

    #[test]
    fn display_task_uri() {
        let uri = BrainUri::for_task("my-project", "BRN-01DEF");
        assert_eq!(uri.to_string(), "brain://my-project/task/BRN-01DEF");
    }

    #[test]
    fn display_memory_uri() {
        let uri = BrainUri::for_memory("my-project", "chunk-abc123");
        assert_eq!(uri.to_string(), "brain://my-project/memory/chunk-abc123");
    }

    #[test]
    fn display_episode_uri() {
        let uri = BrainUri::for_episode("my-project", "01GHI");
        assert_eq!(uri.to_string(), "brain://my-project/episode/01GHI");
    }

    #[test]
    fn display_reflection_uri() {
        let uri = BrainUri::for_reflection("my-project", "01JKL");
        assert_eq!(uri.to_string(), "brain://my-project/reflection/01JKL");
    }

    #[test]
    fn display_procedure_uri() {
        let uri = BrainUri::for_procedure("my-project", "01MNO");
        assert_eq!(uri.to_string(), "brain://my-project/procedure/01MNO");
    }

    #[test]
    fn parse_display_roundtrip_all_domains() {
        let inputs = [
            "brain://my-project/record/BRN-01ABC",
            "brain://my-project/task/BRN-01DEF",
            "brain://my-project/memory/chunk-abc123",
            "brain://my-project/episode/01GHI",
            "brain://my-project/reflection/01JKL",
            "brain://my-project/procedure/01MNO",
        ];
        for input in inputs {
            let uri = BrainUri::parse(input).unwrap();
            assert_eq!(uri.to_string(), input, "round-trip failed for {input}");
        }
    }

    // -----------------------------------------------------------------------
    // BrainUri::parse — error cases
    // -----------------------------------------------------------------------

    #[test]
    fn parse_missing_scheme_returns_error() {
        assert!(BrainUri::parse("my-project/record/BRN-01ABC").is_err());
        assert!(BrainUri::parse("http://my-project/record/BRN-01ABC").is_err());
        assert!(BrainUri::parse("//my-project/record/BRN-01ABC").is_err());
    }

    #[test]
    fn parse_empty_string_returns_error() {
        assert!(BrainUri::parse("").is_err());
    }

    #[test]
    fn parse_unknown_domain_returns_error() {
        assert!(BrainUri::parse("brain://my-project/unknown/BRN-01ABC").is_err());
        assert!(BrainUri::parse("brain://my-project/note/BRN-01ABC").is_err());
        assert!(BrainUri::parse("brain://my-project/chunk/BRN-01ABC").is_err());
    }

    #[test]
    fn parse_missing_brain_returns_error() {
        // brain:// with empty host
        assert!(BrainUri::parse("brain:///record/BRN-01ABC").is_err());
    }

    #[test]
    fn parse_missing_id_returns_error() {
        // only two path segments
        assert!(BrainUri::parse("brain://my-project/record").is_err());
        assert!(BrainUri::parse("brain://my-project/record/").is_err());
    }

    #[test]
    fn parse_missing_domain_returns_error() {
        // only one path segment (brain only)
        assert!(BrainUri::parse("brain://my-project").is_err());
        assert!(BrainUri::parse("brain://my-project/").is_err());
    }

    #[test]
    fn parse_too_many_segments_returns_error() {
        // extra trailing segment
        assert!(BrainUri::parse("brain://my-project/record/BRN-01ABC/extra").is_err());
    }

    #[test]
    fn parse_empty_brain_name_returns_error() {
        assert!(BrainUri::parse("brain:///record/BRN-01ABC").is_err());
    }

    // -----------------------------------------------------------------------
    // Convenience constructors
    // -----------------------------------------------------------------------

    #[test]
    fn for_record_constructor() {
        let uri = BrainUri::for_record("default", "BRN-XYZZY");
        assert_eq!(uri.brain, "default");
        assert_eq!(uri.domain, Domain::Record);
        assert_eq!(uri.id, "BRN-XYZZY");
    }

    #[test]
    fn for_task_constructor() {
        let uri = BrainUri::for_task("default", "BRN-XYZZY");
        assert_eq!(uri.domain, Domain::Task);
    }

    #[test]
    fn for_memory_constructor() {
        let uri = BrainUri::for_memory("default", "chunk-001");
        assert_eq!(uri.domain, Domain::Memory);
    }

    #[test]
    fn for_episode_constructor() {
        let uri = BrainUri::for_episode("default", "01ABC");
        assert_eq!(uri.domain, Domain::Episode);
    }

    #[test]
    fn for_reflection_constructor() {
        let uri = BrainUri::for_reflection("default", "01DEF");
        assert_eq!(uri.domain, Domain::Reflection);
    }

    #[test]
    fn for_procedure_constructor() {
        let uri = BrainUri::for_procedure("default", "01GHI");
        assert_eq!(uri.domain, Domain::Procedure);
    }

    #[test]
    fn convenience_constructors_produce_valid_display() {
        assert_eq!(
            BrainUri::for_record("proj", "R-001").to_string(),
            "brain://proj/record/R-001"
        );
        assert_eq!(
            BrainUri::for_task("proj", "T-002").to_string(),
            "brain://proj/task/T-002"
        );
        assert_eq!(
            BrainUri::for_memory("proj", "C-003").to_string(),
            "brain://proj/memory/C-003"
        );
        assert_eq!(
            BrainUri::for_episode("proj", "E-004").to_string(),
            "brain://proj/episode/E-004"
        );
        assert_eq!(
            BrainUri::for_reflection("proj", "RF-005").to_string(),
            "brain://proj/reflection/RF-005"
        );
        assert_eq!(
            BrainUri::for_procedure("proj", "P-006").to_string(),
            "brain://proj/procedure/P-006"
        );
    }

    // -----------------------------------------------------------------------
    // Struct field access
    // -----------------------------------------------------------------------

    #[test]
    fn brain_uri_fields_are_public() {
        let uri = BrainUri {
            brain: "test-brain".to_string(),
            domain: Domain::Task,
            id: "TSK-001".to_string(),
        };
        assert_eq!(uri.brain, "test-brain");
        assert_eq!(uri.domain, Domain::Task);
        assert_eq!(uri.id, "TSK-001");
    }
}
