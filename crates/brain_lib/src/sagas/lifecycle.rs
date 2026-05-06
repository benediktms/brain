use brain_persistence::error::{BrainCoreError, Result};

use super::status::SagaStatus;

/// Validate a lifecycle transition. Returns `Ok(())` for the 7 valid edges;
/// returns an error for all 9 forbidden transitions.
///
/// This function is the single source of truth for the saga state machine.
/// Tickets .7 (close), .8 (cancel), .9 (reopen) MUST NOT modify this function —
/// they only emit their respective event and call this validator.
///
/// Valid edges (7):
///   planning  → open        (start)
///   open      → closed      (close)
///   planning  → cancelled   (cancel from planning)
///   open      → cancelled   (cancel from open)
///   closed    → cancelled   (cancel from closed)
///   closed    → open        (reopen)
///   cancelled → open        (reopen)
pub fn validate_transition(from: SagaStatus, to: SagaStatus) -> Result<()> {
    use SagaStatus::*;
    match (from, to) {
        (Planning, Open) => Ok(()),
        (Open, Closed) => Ok(()),
        (Planning, Cancelled) => Ok(()),
        (Open, Cancelled) => Ok(()),
        (Closed, Cancelled) => Ok(()),
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
                | (Closed, Cancelled)
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

        assert_eq!(valid_count, 7, "exactly 7 valid edges");
        assert_eq!(forbidden_count, 9, "exactly 9 forbidden transitions");
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
    fn cancel_from_closed() {
        assert!(validate_transition(Closed, Cancelled).is_ok());
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
