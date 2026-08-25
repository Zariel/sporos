use std::error::Error;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FailureClass {
    Transient,
    Permanent,
}

#[derive(Debug, Serialize, Deserialize)]
struct ActivityFailure {
    class: FailureClass,
    code: String,
    message: String,
}

pub(crate) fn transient(code: &str, error: &(dyn Error + 'static)) -> String {
    encode(FailureClass::Transient, code, error)
}

pub(crate) fn permanent(code: &str, error: &(dyn Error + 'static)) -> String {
    encode(FailureClass::Permanent, code, error)
}

pub(crate) fn retryable(raw: &str) -> bool {
    serde_json::from_str::<ActivityFailure>(raw)
        .is_ok_and(|failure| matches!(failure.class, FailureClass::Transient))
}

pub(crate) fn permanent_reason(raw: &str) -> Option<String> {
    let failure = serde_json::from_str::<ActivityFailure>(raw).ok()?;
    matches!(failure.class, FailureClass::Permanent).then_some(failure.code)
}

fn encode(class: FailureClass, code: &str, error: &(dyn Error + 'static)) -> String {
    serde_json::to_string(&ActivityFailure {
        class,
        code: code.to_owned(),
        message: crate::error_report::ErrorReport::new(error).to_string(),
    })
    .expect("activity failure fields are serializable")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_structured_failures() {
        let transient_error = std::io::Error::other("busy");
        let permanent_error = std::io::Error::other("bad payload");
        let transient = transient("database_unavailable", &transient_error);
        let permanent = permanent("invalid_input", &permanent_error);

        assert!(retryable(&transient));
        assert!(!retryable(&permanent));
        assert!(!retryable("legacy error"));
        assert_eq!(
            permanent_reason(&permanent).as_deref(),
            Some("invalid_input")
        );
    }

    #[test]
    fn preserves_the_complete_error_chain() {
        let source = std::io::Error::other("database is locked");
        let error = FixtureError(source);

        let encoded = transient("database_unavailable", &error);
        let failure: ActivityFailure = serde_json::from_str(&encoded).unwrap();

        assert_eq!(failure.message, "projection failed: database is locked");
    }

    #[derive(Debug, thiserror::Error)]
    #[error("projection failed")]
    struct FixtureError(#[source] std::io::Error);
}
