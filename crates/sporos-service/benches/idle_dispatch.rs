use std::hint::black_box;
use std::num::NonZeroU32;
use std::time::{Duration, Instant};

use duroxide::providers::sqlite::{SqliteOptions, SqliteProvider, SqliteSynchronous};
use duroxide::providers::{DispatcherCapabilityFilter, Provider, SessionFetchConfig, TagFilter};

const ORCHESTRATION_DISPATCHERS: usize = 14;
const ACTIVITY_DISPATCHERS: usize = 18;
const CYCLES: usize = 250;

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime");
    runtime.block_on(run());
}

async fn run() {
    let database = tempfile::NamedTempFile::new().expect("benchmark database");
    let database_url = format!("sqlite://{}", database.path().display());
    let provider = SqliteProvider::new(
        &database_url,
        Some(SqliteOptions {
            synchronous: SqliteSynchronous::Full,
            max_connections: NonZeroU32::new(1).expect("one is non-zero"),
        }),
    )
    .await
    .expect("benchmark provider");
    let capability = DispatcherCapabilityFilter::default_for_current_build();
    let session = SessionFetchConfig {
        owner_id: "benchmark-worker".to_owned(),
        lock_timeout: Duration::from_secs(30),
    };
    let tags = TagFilter::default();

    idle_cycle(&provider, &capability, &session, &tags).await;
    let started = Instant::now();
    for _ in 0..CYCLES {
        idle_cycle(&provider, &capability, &session, &tags).await;
    }
    let elapsed = started.elapsed();
    let operations = CYCLES * (ORCHESTRATION_DISPATCHERS + ACTIVITY_DISPATCHERS);
    let nanos_per_fetch = elapsed.as_nanos() / operations as u128;

    println!(
        "idle_dispatch_14_orchestrations_18_activities: {operations} empty fetches in {elapsed:?} ({nanos_per_fetch} ns/fetch)"
    );
    println!(
        "modeled_idle_fetch_rate: 100ms={} fetches/s, 1s={} fetches/s",
        (ORCHESTRATION_DISPATCHERS + ACTIVITY_DISPATCHERS) * 10,
        ORCHESTRATION_DISPATCHERS + ACTIVITY_DISPATCHERS,
    );
}

async fn idle_cycle(
    provider: &SqliteProvider,
    capability: &DispatcherCapabilityFilter,
    session: &SessionFetchConfig,
    tags: &TagFilter,
) {
    for _ in 0..ORCHESTRATION_DISPATCHERS {
        let item = provider
            .fetch_orchestration_item(Duration::from_secs(5), Duration::ZERO, Some(capability))
            .await
            .expect("idle orchestration fetch");
        black_box(item);
    }
    for _ in 0..ACTIVITY_DISPATCHERS {
        let item = provider
            .fetch_work_item(Duration::from_secs(30), Duration::ZERO, Some(session), tags)
            .await
            .expect("idle activity fetch");
        black_box(item);
    }
}
