#![cfg_attr(
    test,
    expect(
        clippy::let_underscore_must_use,
        reason = "test server teardown intentionally ignores post-test join outcome"
    )
)]

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex as AsyncMutex;

use crate::arr::{ArrEndpoint, ArrHttpClient, ArrLookupOutcome};
use crate::domain::{DependencyName, DependencyState, LocalItem, ReasonText};
use crate::errors::DatabaseError;
use crate::indexers::TorznabCaps;
use crate::matching::{
    SearchCacheKey, SearchIds, TorznabSearchPlan, plan_torznab_search, search_cache_key,
};
use crate::persistence::repository::{DependencyHealthSnapshot, Repository};
use crate::runtime::health::{DependencyKind, HealthRegistry};

#[derive(Debug, Clone)]
pub struct RuntimeSearchPlanner {
    repository: Repository,
    health: HealthRegistry,
    arr_client: ArrHttpClient,
    arr_endpoints: Arc<AsyncMutex<Vec<ArrEndpoint>>>,
}

impl RuntimeSearchPlanner {
    pub fn new(
        repository: Repository,
        health: HealthRegistry,
        arr_endpoints: Vec<ArrEndpoint>,
        arr_timeout: Duration,
    ) -> Self {
        Self {
            repository,
            health,
            arr_client: ArrHttpClient::new(arr_timeout),
            arr_endpoints: Arc::new(AsyncMutex::new(arr_endpoints)),
        }
    }

    pub fn with_arr_client(mut self, arr_client: ArrHttpClient) -> Self {
        self.arr_client = arr_client;
        self
    }

    pub async fn plan_torznab_search(
        &self,
        item: &LocalItem,
        caps: &TorznabCaps,
        now_ms: i64,
    ) -> Result<Option<RuntimeTorznabSearchPlan>, DatabaseError> {
        let ids = self.lookup_ids_for_item(item, now_ms).await?;
        Ok(plan_runtime_torznab_search(item, &ids, caps))
    }

    pub async fn lookup_ids_for_item(
        &self,
        item: &LocalItem,
        now_ms: i64,
    ) -> Result<SearchIds, DatabaseError> {
        let endpoints = self.arr_endpoints.lock().await.clone();
        let lookup = self
            .arr_client
            .lookup_ids(&endpoints, item.media_type, &item.title, now_ms)
            .await;
        self.apply_arr_attempts(&lookup.attempts, now_ms).await?;
        Ok(lookup.ids)
    }

    async fn apply_arr_attempts(
        &self,
        attempts: &[crate::arr::ArrLookupAttempt],
        now_ms: i64,
    ) -> Result<(), DatabaseError> {
        for attempt in attempts {
            match &attempt.outcome {
                ArrLookupOutcome::Found { .. } | ArrLookupOutcome::Empty => {
                    self.record_arr_health(
                        &attempt.name,
                        &DependencyState::Healthy {
                            checked_at_ms: now_ms,
                        },
                        now_ms,
                    )
                    .await?;
                    self.update_endpoint(&attempt.name, None, true).await;
                }
                ArrLookupOutcome::Backoff { .. } => {}
                ArrLookupOutcome::Failure {
                    retry_after_ms,
                    reason,
                    unavailable,
                } => {
                    let reason = ReasonText::new(reason.clone()).map_err(|error| {
                        DatabaseError::QueryFailed {
                            operation: "build Arr failure reason".to_owned(),
                            message: error.to_string(),
                        }
                    })?;
                    let state = if *unavailable {
                        DependencyState::Unavailable {
                            reason,
                            retry_after_ms: Some(*retry_after_ms),
                        }
                    } else {
                        DependencyState::Degraded {
                            reason,
                            retry_after_ms: Some(*retry_after_ms),
                        }
                    };
                    self.record_arr_health(&attempt.name, &state, now_ms)
                        .await?;
                    self.update_endpoint(&attempt.name, Some(*retry_after_ms), false)
                        .await;
                }
            }
        }

        Ok(())
    }

    async fn record_arr_health(
        &self,
        name: &DependencyName,
        state: &DependencyState,
        checked_at_ms: i64,
    ) -> Result<(), DatabaseError> {
        match state {
            DependencyState::Unknown => {
                self.health.set_unknown(DependencyKind::Arr, name.clone());
            }
            DependencyState::Healthy { checked_at_ms } => {
                self.health
                    .set_healthy(DependencyKind::Arr, name.clone(), *checked_at_ms);
            }
            DependencyState::Degraded {
                reason,
                retry_after_ms,
            } => {
                self.health.set_degraded(
                    DependencyKind::Arr,
                    name.clone(),
                    reason.clone(),
                    *retry_after_ms,
                );
            }
            DependencyState::Unavailable {
                reason,
                retry_after_ms,
            } => {
                self.health.set_unavailable(
                    DependencyKind::Arr,
                    name.clone(),
                    reason.clone(),
                    *retry_after_ms,
                );
            }
        }
        self.repository
            .record_dependency_health("arr", name, state, checked_at_ms)
            .await
    }

    async fn update_endpoint(
        &self,
        name: &DependencyName,
        retry_after_ms: Option<i64>,
        reset_failures: bool,
    ) {
        let mut endpoints = self.arr_endpoints.lock().await;
        for endpoint in endpoints
            .iter_mut()
            .filter(|endpoint| endpoint.name == *name)
        {
            endpoint.retry_after_ms = retry_after_ms;
            if reset_failures {
                endpoint.consecutive_failures = 0;
            } else {
                endpoint.consecutive_failures = endpoint.consecutive_failures.saturating_add(1);
            }
        }
    }
}

pub(crate) fn seed_arr_endpoint_backoff(
    endpoints: &mut [ArrEndpoint],
    rows: &[DependencyHealthSnapshot],
    now_ms: i64,
) {
    for endpoint in endpoints {
        let Some(row) = rows.iter().find(|row| {
            row.dependency_type == "arr" && row.dependency_name.as_str() == endpoint.name.as_str()
        }) else {
            continue;
        };
        if matches!(row.state.as_str(), "degraded" | "unavailable") {
            endpoint.consecutive_failures = row.failure_count;
            if row
                .retry_after_ms
                .is_some_and(|retry_after| retry_after > now_ms)
            {
                endpoint.retry_after_ms = row.retry_after_ms;
            }
        }
    }
}

pub fn plan_runtime_torznab_search(
    item: &LocalItem,
    ids: &SearchIds,
    caps: &TorznabCaps,
) -> Option<RuntimeTorznabSearchPlan> {
    let plan = plan_torznab_search(item, ids, caps)?;
    let cache_key = search_cache_key(item, ids, plan.query.search_type);
    Some(RuntimeTorznabSearchPlan { plan, cache_key })
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RuntimeTorznabSearchPlan {
    pub plan: TorznabSearchPlan,
    pub cache_key: SearchCacheKey,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::convert::Infallible;

    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode as AxumStatusCode};
    use axum::response::{IntoResponse, Response};
    use axum::routing::get;
    use tokio::net::TcpListener;

    use super::*;
    use crate::arr::{ArrKind, SanitizedArrUrl};
    use crate::domain::{ByteSize, DisplayName, ItemTitle, LocalItemSource, MediaType, SourceKey};
    use crate::indexers::{CategoryCaps, IndexerBackoffPolicy, SearchCaps, TorznabLimits};
    use crate::persistence::repository::Repository;
    use crate::runtime::health::{DependencyKey, DependencySummary};
    use crate::secrets::ApiKey;

    #[tokio::test]
    async fn planner_uses_arr_ids_for_torznab_plan_and_cache_key() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let health = HealthRegistry::new();
        let arr = endpoint(
            ArrKind::Radarr,
            spawn_arr_server(|request| async move {
                let has_key = request
                    .headers()
                    .get("X-Api-Key")
                    .and_then(|value| value.to_str().ok())
                    == Some("secret");
                if has_key {
                    (AxumStatusCode::OK, r#"{"movie":{"tmdbId":99}}"#).into_response()
                } else {
                    (AxumStatusCode::UNAUTHORIZED, "{}").into_response()
                }
            })
            .await,
        );
        let planner =
            RuntimeSearchPlanner::new(repository, health, vec![arr], Duration::from_secs(5));

        let planned = planner
            .plan_torznab_search(&local_item("Example.Movie.1080p"), &movie_caps(), 1_000)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(Some("99"), planned.plan.query.ids.tmdb_id.as_deref());
        assert_eq!(None, planned.plan.query.q);
        assert!(planned.cache_key.as_str().contains("tmdb:99"));
    }

    #[tokio::test]
    async fn planner_persists_arr_backoff_and_skips_until_retry() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let health = HealthRegistry::new();
        let arr = endpoint(
            ArrKind::Radarr,
            spawn_arr_server(|_request| async move {
                (
                    AxumStatusCode::TOO_MANY_REQUESTS,
                    [("Retry-After", "5")],
                    "{}",
                )
                    .into_response()
            })
            .await,
        );
        let planner = RuntimeSearchPlanner::new(
            repository.clone(),
            health.clone(),
            vec![arr],
            Duration::from_secs(5),
        );

        let first = planner
            .plan_torznab_search(&local_item("Example.Movie.1080p"), &movie_caps(), 1_000)
            .await
            .unwrap()
            .unwrap();
        let second = planner
            .plan_torznab_search(&local_item("Example.Movie.1080p"), &movie_caps(), 2_000)
            .await
            .unwrap()
            .unwrap();
        let third = planner
            .plan_torznab_search(&local_item("Example.Movie.1080p"), &movie_caps(), 3_000)
            .await
            .unwrap()
            .unwrap();
        let persisted = repository.dependency_health_snapshot(10).await.unwrap();

        assert_eq!(Some("Example.Movie.1080p"), first.plan.query.q.as_deref());
        assert_eq!(Some("Example.Movie.1080p"), second.plan.query.q.as_deref());
        assert_eq!(Some("Example.Movie.1080p"), third.plan.query.q.as_deref());
        assert_eq!(1, persisted.len());
        assert_eq!("arr", persisted[0].dependency_type);
        assert_eq!("degraded", persisted[0].state);
        assert_eq!(Some(6_000), persisted[0].retry_after_ms);
        assert_eq!(1, persisted[0].failure_count);
        let snapshot = health.snapshot();
        assert_eq!(
            Some(&DependencySummary::Degraded),
            snapshot.summaries.get(&DependencyKind::Arr)
        );
        let key = DependencyKey::new(
            DependencyKind::Arr,
            DependencyName::new("radarr-main").unwrap(),
        );
        assert!(matches!(
            health.state(&key),
            Some(DependencyState::Degraded {
                retry_after_ms: Some(6_000),
                ..
            })
        ));
    }

    #[tokio::test]
    async fn planner_restores_arr_failure_count_for_next_backoff() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let health = HealthRegistry::new();
        let name = DependencyName::new("radarr-main").unwrap();
        let state = DependencyState::Unavailable {
            reason: ReasonText::new("Arr instance returned HTTP status 500").unwrap(),
            retry_after_ms: Some(5_000),
        };
        repository
            .record_dependency_health("arr", &name, &state, 100)
            .await
            .unwrap();
        repository
            .record_dependency_health("arr", &name, &state, 200)
            .await
            .unwrap();
        let persisted = repository.dependency_health_snapshot(10).await.unwrap();
        let mut endpoints = vec![endpoint(
            ArrKind::Radarr,
            spawn_arr_server(|_request| async move {
                (AxumStatusCode::INTERNAL_SERVER_ERROR, "{}").into_response()
            })
            .await,
        )];

        seed_arr_endpoint_backoff(&mut endpoints, &persisted, 1_000);
        let planner = RuntimeSearchPlanner::new(
            repository.clone(),
            health,
            endpoints,
            Duration::from_secs(5),
        )
        .with_arr_client(ArrHttpClient::new(Duration::from_secs(5)).with_backoff(
            IndexerBackoffPolicy {
                base_delay_ms: 100,
                max_delay_ms: 10_000,
                jitter_ms: 0,
                recovery_probe_interval_ms: 100,
            },
        ));

        planner
            .plan_torznab_search(&local_item("Example.Movie.1080p"), &movie_caps(), 5_000)
            .await
            .unwrap()
            .unwrap();
        let persisted = repository.dependency_health_snapshot(10).await.unwrap();

        assert_eq!(3, persisted[0].failure_count);
        assert_eq!(Some(5_800), persisted[0].retry_after_ms);
    }

    #[tokio::test]
    async fn planner_restores_arr_failure_count_after_due_backoff() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let health = HealthRegistry::new();
        let name = DependencyName::new("radarr-main").unwrap();
        let state = DependencyState::Unavailable {
            reason: ReasonText::new("Arr instance returned HTTP status 500").unwrap(),
            retry_after_ms: Some(500),
        };
        repository
            .record_dependency_health("arr", &name, &state, 100)
            .await
            .unwrap();
        repository
            .record_dependency_health("arr", &name, &state, 200)
            .await
            .unwrap();
        let persisted = repository.dependency_health_snapshot(10).await.unwrap();
        let mut endpoints = vec![endpoint(
            ArrKind::Radarr,
            spawn_arr_server(|_request| async move {
                (AxumStatusCode::INTERNAL_SERVER_ERROR, "{}").into_response()
            })
            .await,
        )];

        seed_arr_endpoint_backoff(&mut endpoints, &persisted, 1_000);
        let planner = RuntimeSearchPlanner::new(
            repository.clone(),
            health,
            endpoints,
            Duration::from_secs(5),
        )
        .with_arr_client(ArrHttpClient::new(Duration::from_secs(5)).with_backoff(
            IndexerBackoffPolicy {
                base_delay_ms: 100,
                max_delay_ms: 10_000,
                jitter_ms: 0,
                recovery_probe_interval_ms: 100,
            },
        ));

        planner
            .plan_torznab_search(&local_item("Example.Movie.1080p"), &movie_caps(), 1_000)
            .await
            .unwrap()
            .unwrap();
        let persisted = repository.dependency_health_snapshot(10).await.unwrap();

        assert_eq!(3, persisted[0].failure_count);
        assert_eq!(Some(1_800), persisted[0].retry_after_ms);
    }

    fn movie_caps() -> TorznabCaps {
        TorznabCaps {
            search: SearchCaps {
                movie_search: true,
                supported_id_params: BTreeSet::from(["tmdbid".to_owned()]),
                ..SearchCaps::default()
            },
            categories: CategoryCaps {
                movie: true,
                ..CategoryCaps::default()
            },
            limits: TorznabLimits::default(),
        }
    }

    fn local_item(title: &str) -> LocalItem {
        LocalItem {
            id: None,
            source: LocalItemSource::Virtual {
                source_key: SourceKey::new(title).unwrap(),
            },
            title: ItemTitle::new(title).unwrap(),
            display_name: DisplayName::new(title).unwrap(),
            media_type: MediaType::Movie,
            info_hash: None,
            path: None,
            save_path: None,
            total_size: ByteSize::new(1),
            mtime_ms: None,
        }
    }

    fn endpoint(kind: ArrKind, url: String) -> ArrEndpoint {
        ArrEndpoint {
            kind,
            name: DependencyName::new(format!("{}-main", kind.as_str())).unwrap(),
            url: SanitizedArrUrl::new(url).unwrap(),
            api_key: ApiKey::new("secret").unwrap(),
            retry_after_ms: None,
            consecutive_failures: 0,
        }
    }

    async fn spawn_arr_server<F, Fut>(handler: F) -> String
    where
        F: Fn(Request<Body>) -> Fut + Clone + Send + Sync + 'static,
        Fut: std::future::Future<Output = Response> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/api/v3/parse",
            get(move |request| {
                let handler = handler.clone();
                async move { Ok::<_, Infallible>(handler(request).await) }
            }),
        );
        let server = axum::serve(listener, app);
        tokio::spawn(async move {
            let _ = server.await;
        });

        format!("http://{address}")
    }
}
