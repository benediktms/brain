use brain_core::error::{BrainCoreError, Result};

use super::status::SagaStatus;

pub mod members;
pub mod metadata;
pub mod state_changes;

/// Validate a lifecycle transition. Returns `Ok(())` for the 6 valid edges;
/// returns an error for all 10 forbidden transitions.
///
/// This function is the single source of truth for the saga state machine.
///
/// Valid edges (6):
///   planning  → open        (start)
///   open      → closed      (close)
///   planning  → cancelled   (cancel from planning)
///   open      → cancelled   (cancel from open)
///   closed    → open        (reopen)
///   cancelled → open        (reopen)
///
/// Cancel from `closed` is rejected — spec says "cancel any *active* status".
/// Use `reopen` first if you need to flip a closed saga to cancelled.
pub fn validate_transition(from: SagaStatus, to: SagaStatus) -> Result<()> {
    use SagaStatus::*;
    match (from, to) {
        (Planning, Open) => Ok(()),
        (Open, Closed) => Ok(()),
        (Planning, Cancelled) => Ok(()),
        (Open, Cancelled) => Ok(()),
        (Closed, Open) => Ok(()),
        (Cancelled, Open) => Ok(()),
        _ => Err(BrainCoreError::Parse(format!(
            "invalid saga lifecycle transition: {} → {}",
            from.as_str(),
            to.as_str()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use SagaStatus::*;

    const ALL_STATES: [SagaStatus; 4] = [Planning, Open, Closed, Cancelled];

    fn is_valid(from: SagaStatus, to: SagaStatus) -> bool {
        matches!(
            (from, to),
            (Planning, Open)
                | (Open, Closed)
                | (Planning, Cancelled)
                | (Open, Cancelled)
                | (Closed, Open)
                | (Cancelled, Open)
        )
    }

    #[test]
    fn exhaustive_4x4_matrix() {
        let mut valid_count = 0;
        let mut forbidden_count = 0;

        for &from in &ALL_STATES {
            for &to in &ALL_STATES {
                let result = validate_transition(from, to);
                if is_valid(from, to) {
                    assert!(
                        result.is_ok(),
                        "expected Ok for {} → {} but got Err",
                        from.as_str(),
                        to.as_str()
                    );
                    valid_count += 1;
                } else {
                    assert!(
                        result.is_err(),
                        "expected Err for {} → {} but got Ok",
                        from.as_str(),
                        to.as_str()
                    );
                    forbidden_count += 1;
                }
            }
        }

        assert_eq!(valid_count, 6, "exactly 6 valid edges");
        assert_eq!(forbidden_count, 10, "exactly 10 forbidden transitions");
    }

    #[test]
    fn start_planning_to_open() {
        assert!(validate_transition(Planning, Open).is_ok());
    }

    #[test]
    fn close_open_to_closed() {
        assert!(validate_transition(Open, Closed).is_ok());
    }

    #[test]
    fn cancel_from_planning() {
        assert!(validate_transition(Planning, Cancelled).is_ok());
    }

    #[test]
    fn cancel_from_open() {
        assert!(validate_transition(Open, Cancelled).is_ok());
    }

    #[test]
    fn cancel_from_closed_rejected() {
        // Spec: cancel applies only to active states. Closed → Cancelled is
        // rejected; callers must `reopen` first.
        assert!(validate_transition(Closed, Cancelled).is_err());
    }

    #[test]
    fn reopen_from_closed() {
        assert!(validate_transition(Closed, Open).is_ok());
    }

    #[test]
    fn reopen_from_cancelled() {
        assert!(validate_transition(Cancelled, Open).is_ok());
    }

    #[test]
    fn error_message_contains_from_and_to() {
        let err = validate_transition(Open, Planning).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("open"), "error should mention 'open': {msg}");
        assert!(
            msg.contains("planning"),
            "error should mention 'planning': {msg}"
        );
    }
}
