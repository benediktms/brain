use std::str::FromStr;

use brain_core::error::BrainCoreError;

/// Typed representation of a saga's lifecycle state.
///
/// The DB column stays `TEXT`; this enum is the Rust-side type used in the
/// `Saga` DTO and in all store operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SagaStatus {
    Planning,
    Open,
    Closed,
    Cancelled,
}

impl SagaStatus {
    /// Return the canonical DB string for this status.
    pub fn as_str(&self) -> &'static str {
        match self {
            SagaStatus::Planning => "planning",
            SagaStatus::Open => "open",
            SagaStatus::Closed => "closed",
            SagaStatus::Cancelled => "cancelled",
        }
    }
}

impl std::fmt::Display for SagaStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SagaStatus {
    type Err = BrainCoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "planning" => Ok(Self::Planning),
            "open" => Ok(Self::Open),
            "closed" => Ok(Self::Closed),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(BrainCoreError::Parse(format!(
                "unknown saga status: {other:?}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_variants_round_trip() {
        for status in [
            SagaStatus::Planning,
            SagaStatus::Open,
            SagaStatus::Closed,
            SagaStatus::Cancelled,
        ] {
            let s = status.as_str();
            let parsed: SagaStatus = s.parse().expect("round-trip parse failed");
            assert_eq!(parsed, status, "round-trip failed for {s}");
        }
    }

    #[test]
    fn invalid_status_returns_error() {
        let result = "unknown".parse::<SagaStatus>();
        assert!(result.is_err(), "expected error for invalid status");
    }
}
