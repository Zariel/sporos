use std::fmt::Display;

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

pub(crate) fn transient(code: &str, error: impl Display) -> String {
    encode(FailureClass::Transient, code, error)
}

pub(crate) fn permanent(code: &str, error: impl Display) -> String {
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

fn encode(class: FailureClass, code: &str, error: impl Display) -> String {
    serde_json::to_string(&ActivityFailure {
        class,
        code: code.to_owned(),
        message: error.to_string(),
    })
    .expect("activity failure fields are serializable")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_structured_failures() {
        let transient = transient("database_unavailable", "busy");
        let permanent = permanent("invalid_input", "bad payload");

        assert!(retryable(&transient));
        assert!(!retryable(&permanent));
        assert!(!retryable("legacy error"));
        assert_eq!(
            permanent_reason(&permanent).as_deref(),
            Some("invalid_input")
        );
    }
}
