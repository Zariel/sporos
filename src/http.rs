use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;

use crate::runtime::health::{DependencyHealthSnapshot, HealthRegistry};

#[derive(Debug, Clone)]
pub struct HttpState {
    readiness: Arc<RwLock<ReadinessState>>,
    health: HealthRegistry,
}

impl HttpState {
    pub fn new(readiness: ReadinessState, health: HealthRegistry) -> Self {
        Self {
            readiness: Arc::new(RwLock::new(readiness)),
            health,
        }
    }

    pub fn set_readiness(&self, readiness: ReadinessState) {
        let mut current = self
            .readiness
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *current = readiness;
    }

    fn readiness(&self) -> ReadinessState {
        self.readiness
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn dependency_health(&self) -> DependencyHealthSnapshot {
        self.health.snapshot()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ReadinessState {
    pub config_loaded: bool,
    pub database_available: bool,
    pub schema_initialized: bool,
    pub state_paths_writable: bool,
    pub workers_running: bool,
}

impl ReadinessState {
    pub const fn ready() -> Self {
        Self {
            config_loaded: true,
            database_available: true,
            schema_initialized: true,
            state_paths_writable: true,
            workers_running: true,
        }
    }

    pub const fn is_ready(&self) -> bool {
        self.config_loaded
            && self.database_available
            && self.schema_initialized
            && self.state_paths_writable
            && self.workers_running
    }
}

#[derive(Debug, Serialize)]
struct LivenessResponse {
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct ReadinessResponse {
    status: &'static str,
    checks: ReadinessChecks,
    dependencies: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    status: &'static str,
    readiness: ReadinessResponse,
}

#[derive(Debug, Serialize)]
struct ReadinessChecks {
    config_loaded: bool,
    database_available: bool,
    schema_initialized: bool,
    state_paths_writable: bool,
    workers_running: bool,
}

pub fn router(state: HttpState) -> Router {
    Router::new()
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .route("/v1/status", get(status))
        .with_state(state)
}

async fn livez() -> impl IntoResponse {
    (StatusCode::OK, Json(LivenessResponse { status: "live" }))
}

async fn readyz(State(state): State<HttpState>) -> impl IntoResponse {
    let readiness = readiness_response(&state);
    let status = if readiness.status == "ready" {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(readiness))
}

async fn status(State(state): State<HttpState>) -> impl IntoResponse {
    let readiness = readiness_response(&state);
    (
        StatusCode::OK,
        Json(StatusResponse {
            status: "ok",
            readiness,
        }),
    )
}

fn readiness_response(state: &HttpState) -> ReadinessResponse {
    let readiness = state.readiness();
    ReadinessResponse {
        status: if readiness.is_ready() {
            "ready"
        } else {
            "not_ready"
        },
        checks: ReadinessChecks {
            config_loaded: readiness.config_loaded,
            database_available: readiness.database_available,
            schema_initialized: readiness.schema_initialized,
            state_paths_writable: readiness.state_paths_writable,
            workers_running: readiness.workers_running,
        },
        dependencies: state
            .dependency_health()
            .summaries
            .into_iter()
            .map(|(kind, summary)| (kind.as_str().to_owned(), summary.as_str().to_owned()))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use serde_json::Value;
    use tower::ServiceExt;

    use crate::domain::{DependencyName, ReasonText};
    use crate::runtime::health::DependencyKind;

    #[tokio::test]
    async fn livez_does_not_depend_on_external_health() {
        let health = HealthRegistry::new();
        health.set_unavailable(
            DependencyKind::Indexer,
            DependencyName::new("torznab").unwrap(),
            ReasonText::new("rate limited").unwrap(),
            None,
        );
        let app = router(HttpState::new(ReadinessState::ready(), health));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/livez")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(StatusCode::OK, response.status());
    }

    #[tokio::test]
    async fn readyz_reflects_local_readiness_and_includes_dependencies() {
        let health = HealthRegistry::new();
        health.set_degraded(
            DependencyKind::TorrentClient,
            DependencyName::new("qbit").unwrap(),
            ReasonText::new("auth failed").unwrap(),
            None,
        );
        let state = HttpState::new(
            ReadinessState {
                config_loaded: true,
                database_available: false,
                schema_initialized: true,
                state_paths_writable: true,
                workers_running: true,
            },
            health,
        );
        let app = router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(StatusCode::SERVICE_UNAVAILABLE, status);
        assert_eq!("not_ready", json["status"]);
        assert_eq!(false, json["checks"]["database_available"]);
        assert_eq!("degraded", json["dependencies"]["torrent_client"]);
    }

    #[tokio::test]
    async fn status_route_returns_typed_status_body() {
        let app = router(HttpState::new(
            ReadinessState::ready(),
            HealthRegistry::new(),
        ));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(StatusCode::OK, status);
        assert_eq!("ok", json["status"]);
        assert_eq!("ready", json["readiness"]["status"]);
    }
}
