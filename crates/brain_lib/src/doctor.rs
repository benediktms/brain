use std::fmt;

/// Status of a single diagnostic check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckStatus {
    Ok,
    Warning,
    Problem,
}

impl fmt::Display for CheckStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CheckStatus::Ok => write!(f, "OK"),
            CheckStatus::Warning => write!(f, "WARN"),
            CheckStatus::Problem => write!(f, "PROBLEM"),
        }
    }
}

/// A single diagnostic check result.
#[derive(Debug, Clone)]
pub struct Check {
    pub name: String,
    pub status: CheckStatus,
    pub detail: String,
}

/// Aggregated results from running all doctor checks.
#[derive(Debug, Default)]
pub struct DoctorReport {
    pub checks: Vec<Check>,
}

impl DoctorReport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, name: impl Into<String>, status: CheckStatus, detail: impl Into<String>) {
        self.checks.push(Check {
            name: name.into(),
            status,
            detail: detail.into(),
        });
    }

    /// True if all checks passed (no warnings or problems).
    pub fn is_healthy(&self) -> bool {
        self.checks
            .iter()
            .all(|c| c.status == CheckStatus::Ok)
    }

    /// Count of checks with Problem status.
    pub fn problem_count(&self) -> usize {
        self.checks
            .iter()
            .filter(|c| c.status == CheckStatus::Problem)
            .count()
    }

    /// Count of checks with Warning status.
    pub fn warning_count(&self) -> usize {
        self.checks
            .iter()
            .filter(|c| c.status == CheckStatus::Warning)
            .count()
    }
}

impl fmt::Display for DoctorReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for check in &self.checks {
            let icon = match check.status {
                CheckStatus::Ok => "  OK",
                CheckStatus::Warning => "WARN",
                CheckStatus::Problem => "FAIL",
            };
            writeln!(f, "[{icon}] {}: {}", check.name, check.detail)?;
        }
        writeln!(f)?;
        let total = self.checks.len();
        let problems = self.problem_count();
        let warnings = self.warning_count();
        let ok = total - problems - warnings;
        write!(
            f,
            "{total} checks: {ok} passed, {warnings} warnings, {problems} problems"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_report_is_healthy() {
        let report = DoctorReport::new();
        assert!(report.is_healthy());
        assert_eq!(report.problem_count(), 0);
        assert_eq!(report.warning_count(), 0);
    }

    #[test]
    fn test_report_with_problems() {
        let mut report = DoctorReport::new();
        report.add("check1", CheckStatus::Ok, "all good");
        report.add("check2", CheckStatus::Problem, "something wrong");
        report.add("check3", CheckStatus::Warning, "heads up");

        assert!(!report.is_healthy());
        assert_eq!(report.problem_count(), 1);
        assert_eq!(report.warning_count(), 1);
    }

    #[test]
    fn test_report_display() {
        let mut report = DoctorReport::new();
        report.add("Orphan chunks", CheckStatus::Ok, "0 orphan chunks");
        report.add("Stuck files", CheckStatus::Problem, "2 files stuck");

        let output = report.to_string();
        assert!(output.contains("[  OK] Orphan chunks: 0 orphan chunks"));
        assert!(output.contains("[FAIL] Stuck files: 2 files stuck"));
        assert!(output.contains("1 passed"));
        assert!(output.contains("1 problems"));
    }
}
