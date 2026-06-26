use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use duroxide::providers::Provider;
use duroxide::providers::sqlite::SqliteProvider;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{OrchestrationStatus, Runtime, RuntimeOptions};
use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry};
use serde::{Deserialize, Serialize};

use crate::domain::DependencyName;
use crate::runtime::workflow_contracts::{
    ActivityKind, WorkflowCustomStatus, WorkflowInstanceId, WorkflowKind, WorkflowReason,
    WorkflowState,
};

pub const WORKFLOW_RUNTIME_DEPENDENCY: &str = "workflow-runtime";
const WORKFLOW_DATABASE_FILE: &str = "sporos-workflows.db";
const DEFAULT_DATABASE_DIR: &str = "db";
const STARTUP_POLL_INTERVAL: Duration = Duration::from_millis(10);
const STARTUP_LONG_POLL_TIMEOUT: Duration = Duration::from_millis(50);

#[derive(Clone)]
pub struct DuroxideWorkflowRuntime {
    database_path: PathBuf,
    store: Arc<dyn Provider>,
    runtime: Arc<Runtime>,
    seeded_supervisors: Arc<Mutex<BTreeSet<String>>>,
}

impl DuroxideWorkflowRuntime {
    pub async fn start(database_path: PathBuf) -> Result<Self, DuroxideWorkflowRuntimeError> {
        prepare_workflow_database(&database_path).await?;
        let database_url = format!("sqlite:{}", database_path.display());
        let store = Arc::new(
            SqliteProvider::new(&database_url, None)
                .await
                .map_err(|error| DuroxideWorkflowRuntimeError::OpenDatabase {
                    path: database_path.clone(),
                    message: error.to_string(),
                })?,
        ) as Arc<dyn Provider>;
        let runtime = Runtime::start_with_options(
            Arc::clone(&store),
            activity_registry(),
            orchestration_registry(),
            RuntimeOptions {
                dispatcher_min_poll_interval: STARTUP_POLL_INTERVAL,
                dispatcher_long_poll_timeout: STARTUP_LONG_POLL_TIMEOUT,
                orchestration_concurrency: 1,
                worker_concurrency: 1,
                ..RuntimeOptions::default()
            },
        )
        .await;

        Ok(Self {
            database_path,
            store,
            runtime,
            seeded_supervisors: Arc::new(Mutex::new(BTreeSet::new())),
        })
    }

    pub fn database_path(&self) -> &Path {
        &self.database_path
    }

    pub fn client(&self) -> Client {
        Client::new(Arc::clone(&self.store))
    }

    pub async fn seed_supervisors(
        &self,
        scheduled_jobs: &[&str],
    ) -> Result<WorkflowSupervisorSeedSummary, DuroxideWorkflowRuntimeError> {
        let client = self.client();
        let mut summary = WorkflowSupervisorSeedSummary::default();
        for job_name in scheduled_jobs {
            let instance_id = WorkflowInstanceId::scheduled_job_supervisor(job_name)
                .map_err(DuroxideWorkflowRuntimeError::InvalidSupervisorId)?;
            let seeded = self
                .seed_supervisor(
                    &client,
                    instance_id.as_str(),
                    WorkflowKind::ScheduledJob,
                    job_name,
                )
                .await?;
            summary.record(seeded);
        }

        let saved_retry_id = WorkflowInstanceId::saved_retry_supervisor();
        let seeded = self
            .seed_supervisor(
                &client,
                saved_retry_id.as_str(),
                WorkflowKind::SavedTorrentRetry,
                "saved-retry",
            )
            .await?;
        summary.record(seeded);

        Ok(summary)
    }

    pub async fn shutdown(&self, timeout_ms: Option<u64>) {
        self.runtime.clone().shutdown(timeout_ms).await;
    }

    async fn seed_supervisor(
        &self,
        client: &Client,
        instance_id: &str,
        kind: WorkflowKind,
        public_id: &str,
    ) -> Result<SupervisorSeedOutcome, DuroxideWorkflowRuntimeError> {
        if self.already_seeded(instance_id)? {
            return Ok(SupervisorSeedOutcome::AlreadyPresent);
        }
        match client
            .get_orchestration_status(instance_id)
            .await
            .map_err(|error| DuroxideWorkflowRuntimeError::ReadSupervisor {
                instance_id: instance_id.to_owned(),
                message: error.to_string(),
            })? {
            OrchestrationStatus::NotFound => {
                let input = WorkflowSupervisorInput {
                    kind,
                    public_id: public_id.to_owned(),
                };
                client
                    .start_orchestration_typed(instance_id, kind.orchestration_name(), input)
                    .await
                    .map_err(|error| DuroxideWorkflowRuntimeError::StartSupervisor {
                        instance_id: instance_id.to_owned(),
                        message: error.to_string(),
                    })?;
                self.mark_seeded(instance_id)?;
                Ok(SupervisorSeedOutcome::Started)
            }
            OrchestrationStatus::Running { .. } | OrchestrationStatus::Completed { .. } => {
                self.mark_seeded(instance_id)?;
                Ok(SupervisorSeedOutcome::AlreadyPresent)
            }
            OrchestrationStatus::Failed { details, .. } => {
                Err(DuroxideWorkflowRuntimeError::FailedSupervisor {
                    instance_id: instance_id.to_owned(),
                    message: details.display_message().to_string(),
                })
            }
        }
    }

    fn already_seeded(&self, instance_id: &str) -> Result<bool, DuroxideWorkflowRuntimeError> {
        let seeded = self
            .seeded_supervisors
            .lock()
            .map_err(|_error| DuroxideWorkflowRuntimeError::SeedTrackerPoisoned)?;
        Ok(seeded.contains(instance_id))
    }

    fn mark_seeded(&self, instance_id: &str) -> Result<(), DuroxideWorkflowRuntimeError> {
        let mut seeded = self
            .seeded_supervisors
            .lock()
            .map_err(|_error| DuroxideWorkflowRuntimeError::SeedTrackerPoisoned)?;
        seeded.insert(instance_id.to_owned());
        Ok(())
    }
}

impl fmt::Debug for DuroxideWorkflowRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DuroxideWorkflowRuntime")
            .field("database_path", &self.database_path)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct WorkflowSupervisorSeedSummary {
    pub started: usize,
    pub already_present: usize,
}

impl WorkflowSupervisorSeedSummary {
    fn record(&mut self, outcome: SupervisorSeedOutcome) {
        match outcome {
            SupervisorSeedOutcome::Started => self.started += 1,
            SupervisorSeedOutcome::AlreadyPresent => self.already_present += 1,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum SupervisorSeedOutcome {
    Started,
    AlreadyPresent,
}

#[derive(Debug)]
pub enum DuroxideWorkflowRuntimeError {
    InvalidDependencyName {
        message: String,
    },
    PrepareDatabase {
        path: PathBuf,
        message: String,
    },
    OpenDatabase {
        path: PathBuf,
        message: String,
    },
    InvalidSupervisorId(crate::runtime::workflow_contracts::WorkflowContractError),
    ReadSupervisor {
        instance_id: String,
        message: String,
    },
    FailedSupervisor {
        instance_id: String,
        message: String,
    },
    StartSupervisor {
        instance_id: String,
        message: String,
    },
    SeedTrackerPoisoned,
}

impl fmt::Display for DuroxideWorkflowRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDependencyName { message } => {
                write!(
                    formatter,
                    "workflow runtime dependency name is invalid: {message}"
                )
            }
            Self::PrepareDatabase { path, message } => {
                write!(
                    formatter,
                    "prepare workflow database `{}` failed: {message}",
                    path.display()
                )
            }
            Self::OpenDatabase { path, message } => {
                write!(
                    formatter,
                    "open workflow database `{}` failed: {message}",
                    path.display()
                )
            }
            Self::InvalidSupervisorId(error) => write!(formatter, "{error}"),
            Self::ReadSupervisor {
                instance_id,
                message,
            } => write!(
                formatter,
                "read workflow supervisor `{instance_id}` failed: {message}"
            ),
            Self::FailedSupervisor {
                instance_id,
                message,
            } => write!(
                formatter,
                "workflow supervisor `{instance_id}` is failed: {message}"
            ),
            Self::StartSupervisor {
                instance_id,
                message,
            } => write!(
                formatter,
                "start workflow supervisor `{instance_id}` failed: {message}"
            ),
            Self::SeedTrackerPoisoned => {
                formatter.write_str("workflow supervisor seed tracker is poisoned")
            }
        }
    }
}

impl std::error::Error for DuroxideWorkflowRuntimeError {}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct WorkflowSupervisorInput {
    kind: WorkflowKind,
    public_id: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct WorkflowSupervisorOutput {
    kind: WorkflowKind,
    public_id: String,
    state: WorkflowState,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct WorkflowShellActivityInput {
    activity: ActivityKind,
    workflow_id: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct WorkflowShellActivityOutput {
    activity: ActivityKind,
    accepted: bool,
}

pub fn workflow_database_path(sporos_database_path: &Path) -> PathBuf {
    let parent = sporos_database_path
        .parent()
        .unwrap_or_else(|| Path::new("."));
    let state_root = if parent
        .file_name()
        .is_some_and(|name| name == DEFAULT_DATABASE_DIR)
    {
        parent.parent().unwrap_or(parent)
    } else {
        parent
    };
    state_root.join(WORKFLOW_DATABASE_FILE)
}

pub fn workflow_runtime_dependency_name() -> Result<DependencyName, DuroxideWorkflowRuntimeError> {
    DependencyName::new(WORKFLOW_RUNTIME_DEPENDENCY).map_err(|error| {
        DuroxideWorkflowRuntimeError::InvalidDependencyName {
            message: error.to_string(),
        }
    })
}

async fn prepare_workflow_database(path: &Path) -> Result<(), DuroxideWorkflowRuntimeError> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|error| {
            DuroxideWorkflowRuntimeError::PrepareDatabase {
                path: path.to_path_buf(),
                message: error.to_string(),
            }
        })?;
    }
    tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .map_err(|error| DuroxideWorkflowRuntimeError::PrepareDatabase {
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;
    Ok(())
}

fn activity_registry() -> ActivityRegistry {
    let mut builder = ActivityRegistry::builder();
    for activity in ActivityKind::ALL {
        builder = builder.register_typed(
            activity.as_str(),
            move |_ctx: ActivityContext, input: WorkflowShellActivityInput| async move {
                Ok(WorkflowShellActivityOutput {
                    activity: input.activity,
                    accepted: input.activity == activity,
                })
            },
        );
    }
    builder.build()
}

fn orchestration_registry() -> OrchestrationRegistry {
    let mut builder = OrchestrationRegistry::builder();
    for workflow in WorkflowKind::ALL {
        builder = builder.register_typed(
            workflow.orchestration_name(),
            move |ctx: OrchestrationContext, input: WorkflowSupervisorInput| async move {
                let status = WorkflowCustomStatus::new(
                    input.public_id.clone(),
                    input.kind,
                    WorkflowState::Succeeded,
                    WorkflowReason::Completed,
                );
                let status = serde_json::to_string(&status).map_err(|error| error.to_string())?;
                ctx.set_custom_status(status);
                Ok(WorkflowSupervisorOutput {
                    kind: input.kind,
                    public_id: input.public_id,
                    state: WorkflowState::Succeeded,
                })
            },
        );
    }
    builder.build()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    use duroxide::runtime::OrchestrationStatus;

    use super::*;
    use crate::runtime::scheduler::{
        CLEANUP_JOB_NAME, CLIENT_INVENTORY_JOB_NAME, INDEXER_CAPS_JOB_NAME,
        MEDIA_INVENTORY_JOB_NAME,
    };

    static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn workflow_database_path_uses_state_root_for_default_database_layout() {
        assert_eq!(
            PathBuf::from("/app/state/sporos-workflows.db"),
            workflow_database_path(Path::new("/app/state/db/sporos.db"))
        );
    }

    #[test]
    fn workflow_database_path_uses_database_parent_for_custom_layout() {
        assert_eq!(
            PathBuf::from("/var/lib/sporos/sporos-workflows.db"),
            workflow_database_path(Path::new("/var/lib/sporos/sporos.db"))
        );
    }

    #[tokio::test]
    async fn runtime_starts_seeds_supervisors_idempotently_and_shuts_down() {
        let temp_dir = TestTempDir::new("duroxide-workflow-runtime");
        let database_path = temp_dir.path().join("state").join(WORKFLOW_DATABASE_FILE);
        let runtime = DuroxideWorkflowRuntime::start(database_path.clone())
            .await
            .expect("workflow runtime should start");

        assert_eq!(database_path, runtime.database_path());
        assert!(database_path.exists());

        let scheduled_jobs = [
            CLEANUP_JOB_NAME,
            MEDIA_INVENTORY_JOB_NAME,
            CLIENT_INVENTORY_JOB_NAME,
            INDEXER_CAPS_JOB_NAME,
        ];
        let first_summary = runtime
            .seed_supervisors(&scheduled_jobs)
            .await
            .expect("first supervisor seed should succeed");
        assert_eq!(
            WorkflowSupervisorSeedSummary {
                started: 5,
                already_present: 0
            },
            first_summary
        );

        let second_summary = runtime
            .seed_supervisors(&scheduled_jobs)
            .await
            .expect("second supervisor seed should succeed");
        assert_eq!(
            WorkflowSupervisorSeedSummary {
                started: 0,
                already_present: 5
            },
            second_summary
        );

        let cleanup_id = WorkflowInstanceId::scheduled_job_supervisor(CLEANUP_JOB_NAME).unwrap();
        wait_for_supervisor_completion(&runtime.client(), cleanup_id.as_str()).await;

        runtime.shutdown(Some(1_000)).await;
    }

    #[tokio::test]
    async fn runtime_rejects_failed_supervisor_as_not_seeded() {
        let temp_dir = TestTempDir::new("duroxide-workflow-runtime-failed-supervisor");
        let database_path = temp_dir.path().join("state").join(WORKFLOW_DATABASE_FILE);
        let runtime = DuroxideWorkflowRuntime::start(database_path)
            .await
            .expect("workflow runtime should start");
        let failed_job = "failed_job";
        let failed_id = WorkflowInstanceId::scheduled_job_supervisor(failed_job).unwrap();

        runtime
            .client()
            .start_orchestration_typed(
                failed_id.as_str(),
                WorkflowKind::ScheduledJob.orchestration_name(),
                "not a supervisor input".to_owned(),
            )
            .await
            .expect("failed supervisor should be queued");
        wait_for_supervisor_failure(&runtime.client(), failed_id.as_str()).await;

        let error = runtime
            .seed_supervisors(&[failed_job])
            .await
            .expect_err("failed supervisor must not be treated as seeded");
        assert!(matches!(
            error,
            DuroxideWorkflowRuntimeError::FailedSupervisor { .. }
        ));

        runtime.shutdown(Some(1_000)).await;
    }

    #[test]
    fn workflow_runtime_dependency_name_is_stable() {
        assert_eq!(
            WORKFLOW_RUNTIME_DEPENDENCY,
            workflow_runtime_dependency_name().unwrap().as_str()
        );
    }

    async fn wait_for_supervisor_completion(client: &Client, instance_id: &str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match client
                .get_orchestration_status(instance_id)
                .await
                .expect("supervisor status should be readable")
            {
                OrchestrationStatus::Completed { .. } => return,
                OrchestrationStatus::Failed { details, .. } => {
                    panic!(
                        "supervisor failed unexpectedly: {}",
                        details.display_message()
                    );
                }
                OrchestrationStatus::NotFound | OrchestrationStatus::Running { .. } => {}
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for supervisor completion"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn wait_for_supervisor_failure(client: &Client, instance_id: &str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match client
                .get_orchestration_status(instance_id)
                .await
                .expect("supervisor status should be readable")
            {
                OrchestrationStatus::Failed { .. } => return,
                OrchestrationStatus::Completed { .. } => {
                    panic!("supervisor completed unexpectedly");
                }
                OrchestrationStatus::NotFound | OrchestrationStatus::Running { .. } => {}
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for supervisor failure"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    struct TestTempDir {
        path: PathBuf,
    }

    impl TestTempDir {
        fn new(label: &str) -> Self {
            let unique = TEMP_DIR_COUNTER.fetch_add(1, Ordering::SeqCst);
            let path = std::env::temp_dir()
                .join(format!("sporos-{label}-{}-{unique}", std::process::id()));
            std::fs::create_dir_all(&path).expect("test temp directory should be created");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestTempDir {
        fn drop(&mut self) {
            drop(std::fs::remove_dir_all(&self.path));
        }
    }
}
