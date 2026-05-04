//! Daemon job scheduling, concurrency rules, and job status tracking.

use std::{borrow::Cow, sync::Mutex};

use crate::{
    SporosError,
    api::JobResponse,
    config::{Action, RuntimeConfig},
    persistence::{AsyncDatabase, Database},
};

const ONE_DAY_MILLIS: u64 = 86_400_000;
const ONE_HOUR_MILLIS: u64 = 3_600_000;

/// Scheduler job names in compatibility order.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum JobName {
    /// RSS feed scan.
    Rss,
    /// Scheduled search.
    Search,
    /// Indexer capability refresh.
    UpdateIndexerCaps,
    /// Saved torrent injection retry.
    Inject,
    /// Database and cache cleanup.
    Cleanup,
}

impl JobName {
    /// Compatibility string.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rss => "rss",
            Self::Search => "search",
            Self::UpdateIndexerCaps => "updateIndexerCaps",
            Self::Inject => "inject",
            Self::Cleanup => "cleanup",
        }
    }

    /// Parse a job name.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "rss" => Some(Self::Rss),
            "search" => Some(Self::Search),
            "updateIndexerCaps" => Some(Self::UpdateIndexerCaps),
            "inject" => Some(Self::Inject),
            "cleanup" => Some(Self::Cleanup),
            _ => None,
        }
    }
}

/// Runtime config overrides requested for one queued job run.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct JobConfigOverride {
    /// Override exclude_recent_search for this run.
    pub ignore_exclude_recent_search: bool,
    /// Override exclude_older for this run.
    pub ignore_exclude_older: bool,
}

/// One scheduler job.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ScheduledJob {
    /// Job name.
    pub name: JobName,
    /// Whether config enables this job.
    pub enabled: bool,
    /// Cadence in milliseconds.
    pub cadence_millis: u64,
    /// Whether a run is currently active.
    pub is_active: bool,
    /// Force one run ahead of schedule.
    pub run_ahead_of_schedule: bool,
    /// Runtime config overrides for the next queued run.
    pub config_override: JobConfigOverride,
    /// Move persisted last_run forward by one cadence after an early run.
    pub delay_next_run: bool,
    /// Testable count of successful run dispatches.
    pub runs: u64,
}

impl ScheduledJob {
    /// Build an enabled job with a cadence.
    pub const fn new(name: JobName, cadence_millis: u64, enabled: bool) -> Self {
        Self {
            name,
            enabled,
            cadence_millis,
            is_active: false,
            run_ahead_of_schedule: false,
            config_override: JobConfigOverride {
                ignore_exclude_recent_search: false,
                ignore_exclude_older: false,
            },
            delay_next_run: false,
            runs: 0,
        }
    }

    fn run(&mut self) -> Option<JobConfigOverride> {
        if self.is_active {
            return None;
        }
        self.is_active = true;
        self.runs = self.runs.saturating_add(1);
        self.run_ahead_of_schedule = false;
        let config_override = self.config_override;
        self.config_override = JobConfigOverride::default();
        Some(config_override)
    }

    fn finish(&mut self) {
        self.is_active = false;
    }
}

/// Result from one job considered by `check_jobs`.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct JobCheckResult {
    /// Job name.
    pub name: JobName,
    /// Runtime config overrides for this run.
    pub config_override: JobConfigOverride,
    /// `job_log.last_run` value to persist if the dispatched body succeeds.
    pub completion_last_run: Option<i64>,
    /// Whether the job was dispatched.
    pub ran: bool,
    /// Why the job did not run.
    pub skipped: Option<Cow<'static, str>>,
}

/// Stateful scheduler job collection.
#[derive(Debug)]
pub struct Scheduler {
    jobs: Vec<ScheduledJob>,
    check_jobs: Mutex<()>,
}

/// Daemon lifecycle plan derived from runtime config.
#[derive(Debug)]
pub struct DaemonPlan {
    /// Whether the HTTP listener should be started.
    pub serve_http: bool,
    /// Configured scheduler.
    pub scheduler: Scheduler,
}

/// Result from a bounded daemon lifecycle startup pass.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DaemonRun {
    /// Startup indexing hook ran before serving and jobs.
    pub startup_indexed: bool,
    /// HTTP serving would be started.
    pub serving: bool,
    /// Bound listener address when HTTP serving is active.
    pub listen_addr: Option<std::net::SocketAddr>,
    /// First scheduler check results.
    pub jobs: Vec<JobCheckResult>,
}

impl DaemonPlan {
    /// Build daemon serving and job state from runtime config.
    pub fn from_config(config: &RuntimeConfig) -> Self {
        let jobs = vec![
            ScheduledJob::new(
                JobName::Rss,
                config.rss_cadence.unwrap_or_default(),
                config.rss_cadence.is_some(),
            ),
            ScheduledJob::new(
                JobName::Search,
                config.search_cadence.unwrap_or_default(),
                config.search_cadence.is_some(),
            ),
            ScheduledJob::new(
                JobName::UpdateIndexerCaps,
                ONE_DAY_MILLIS,
                !config.torznab.is_empty(),
            ),
            ScheduledJob::new(
                JobName::Inject,
                ONE_HOUR_MILLIS,
                config.action == Action::Inject,
            ),
            ScheduledJob::new(JobName::Cleanup, ONE_DAY_MILLIS, true),
        ];
        Self {
            serve_http: config.listen_port.is_some(),
            scheduler: Scheduler::new(jobs),
        }
    }

    /// Run startup indexing hook and first job check in lifecycle order.
    pub fn run_startup<I>(
        &mut self,
        database: &Database,
        now_millis: i64,
        mut index_startup: I,
    ) -> crate::Result<DaemonRun>
    where
        I: FnMut() -> crate::Result<()>,
    {
        index_startup()?;
        let jobs = self.scheduler.check_jobs(database, now_millis, true)?;
        Ok(DaemonRun {
            startup_indexed: true,
            serving: self.serve_http,
            listen_addr: None,
            jobs,
        })
    }

    /// Run startup indexing hook and first async job check in lifecycle order.
    pub async fn run_startup_async<I>(
        &mut self,
        database: &AsyncDatabase,
        now_millis: i64,
        mut index_startup: I,
    ) -> crate::Result<DaemonRun>
    where
        I: FnMut() -> crate::Result<()>,
    {
        index_startup()?;
        let jobs = self
            .scheduler
            .check_jobs_async(database, now_millis, true)
            .await?;
        Ok(DaemonRun {
            startup_indexed: true,
            serving: self.serve_http,
            listen_addr: None,
            jobs,
        })
    }
}

impl Scheduler {
    /// Create a scheduler from explicit jobs.
    pub fn new(jobs: Vec<ScheduledJob>) -> Self {
        Self {
            jobs,
            check_jobs: Mutex::new(()),
        }
    }

    /// Borrow configured jobs.
    pub fn jobs(&self) -> &[ScheduledJob] {
        &self.jobs
    }

    /// Mutably borrow configured jobs.
    pub fn jobs_mut(&mut self) -> &mut [ScheduledJob] {
        &mut self.jobs
    }

    /// Mark a job to run ahead of schedule from `/api/job` or webhook behavior.
    pub fn request_early_run(
        &mut self,
        database: &Database,
        name: JobName,
        now_millis: i64,
        config_override: JobConfigOverride,
    ) -> crate::Result<JobResponse> {
        let Some(job) = self.jobs.iter_mut().find(|job| job.name == name) else {
            return Ok(JobResponse::Disabled(format!(
                "{}: unable to run, disabled in config",
                name.as_str()
            )));
        };
        if !job.enabled {
            return Ok(JobResponse::Disabled(format!(
                "{}: unable to run, disabled in config",
                name.as_str()
            )));
        }
        if job.is_active {
            return Ok(JobResponse::AlreadyRunning(format!(
                "{}: already running",
                name.as_str()
            )));
        }
        if read_last_run(database, name)?.is_some_and(|last_run| now_millis < last_run) {
            return Ok(JobResponse::NotEligible(format!(
                "{}: already queued ahead of schedule",
                name.as_str()
            )));
        }
        job.run_ahead_of_schedule = true;
        job.config_override = config_override;
        if matches!(name, JobName::Rss | JobName::Search) {
            job.delay_next_run = true;
        }
        Ok(JobResponse::Accepted(format!(
            "{}: running ahead of schedule",
            name.as_str()
        )))
    }

    /// Mark a job to run ahead of schedule using async scheduler state.
    pub async fn request_early_run_async(
        &mut self,
        database: &AsyncDatabase,
        name: JobName,
        now_millis: i64,
        config_override: JobConfigOverride,
    ) -> crate::Result<JobResponse> {
        let Some(job) = self.jobs.iter_mut().find(|job| job.name == name) else {
            return Ok(JobResponse::Disabled(format!(
                "{}: unable to run, disabled in config",
                name.as_str()
            )));
        };
        if !job.enabled {
            return Ok(JobResponse::Disabled(format!(
                "{}: unable to run, disabled in config",
                name.as_str()
            )));
        }
        if job.is_active {
            return Ok(JobResponse::AlreadyRunning(format!(
                "{}: already running",
                name.as_str()
            )));
        }
        if database
            .read_last_run(name.as_str())
            .await?
            .is_some_and(|last_run| now_millis < last_run)
        {
            return Ok(JobResponse::NotEligible(format!(
                "{}: already queued ahead of schedule",
                name.as_str()
            )));
        }
        job.run_ahead_of_schedule = true;
        job.config_override = config_override;
        if matches!(name, JobName::Rss | JobName::Search) {
            job.delay_next_run = true;
        }
        Ok(JobResponse::Accepted(format!(
            "{}: running ahead of schedule",
            name.as_str()
        )))
    }

    /// Check all jobs using compatibility scheduling and concurrency rules.
    pub fn check_jobs(
        &mut self,
        database: &Database,
        now_millis: i64,
        is_first_run: bool,
    ) -> crate::Result<Vec<JobCheckResult>> {
        let _guard = self
            .check_jobs
            .lock()
            .map_err(|_error| scheduler_error("CHECK_JOBS mutex was poisoned"))?;
        let rss_active = self
            .jobs
            .iter()
            .any(|job| job.name == JobName::Rss && job.is_active);
        let any_active = self.jobs.iter().any(|job| job.is_active);
        let mut results = Vec::with_capacity(self.jobs.len());
        for job in &mut self.jobs {
            let last_run = read_last_run(database, job.name)?;
            if is_first_run {
                tracing::info!(
                    job = job.name.as_str(),
                    last_run = ?last_run,
                    "scheduler job state loaded"
                );
            }
            let eligible = last_run.is_none_or(|last_run| {
                now_millis >= last_run.saturating_add(job.cadence_millis as i64)
            });
            if !job.enabled {
                results.push(JobCheckResult {
                    name: job.name,
                    config_override: JobConfigOverride::default(),
                    completion_last_run: None,
                    ran: false,
                    skipped: Some(Cow::Borrowed("disabled")),
                });
                continue;
            }
            if job.is_active {
                results.push(JobCheckResult {
                    name: job.name,
                    config_override: JobConfigOverride::default(),
                    completion_last_run: None,
                    ran: false,
                    skipped: Some(Cow::Borrowed("already active")),
                });
                continue;
            }
            if !job.run_ahead_of_schedule {
                if rss_active && job.name != JobName::Rss {
                    results.push(JobCheckResult {
                        name: job.name,
                        config_override: JobConfigOverride::default(),
                        completion_last_run: None,
                        ran: false,
                        skipped: Some(Cow::Borrowed("rss active")),
                    });
                    continue;
                }
                if job.name == JobName::Cleanup && any_active {
                    results.push(JobCheckResult {
                        name: job.name,
                        config_override: JobConfigOverride::default(),
                        completion_last_run: None,
                        ran: false,
                        skipped: Some(Cow::Borrowed("another job active")),
                    });
                    continue;
                }
                if !eligible {
                    results.push(JobCheckResult {
                        name: job.name,
                        config_override: JobConfigOverride::default(),
                        completion_last_run: None,
                        ran: false,
                        skipped: Some(Cow::Borrowed("not due")),
                    });
                    continue;
                }
            }
            let config_override = job.run();
            let completion_last_run = if config_override.is_some() {
                Some(if job.delay_next_run {
                    job.delay_next_run = false;
                    now_millis.saturating_add(job.cadence_millis as i64)
                } else {
                    now_millis
                })
            } else {
                None
            };
            if let Some(completion_last_run) = completion_last_run {
                tracing::debug!(
                    job = job.name.as_str(),
                    completion_last_run,
                    "scheduler job dispatch awaiting completion"
                );
            }
            results.push(JobCheckResult {
                name: job.name,
                config_override: config_override.unwrap_or_default(),
                completion_last_run,
                ran: config_override.is_some(),
                skipped: config_override
                    .is_none()
                    .then_some(Cow::Borrowed("already active")),
            });
        }
        Ok(results)
    }

    /// Check all jobs using async scheduler state.
    pub async fn check_jobs_async(
        &mut self,
        database: &AsyncDatabase,
        now_millis: i64,
        is_first_run: bool,
    ) -> crate::Result<Vec<JobCheckResult>> {
        let rss_active = self
            .jobs
            .iter()
            .any(|job| job.name == JobName::Rss && job.is_active);
        let any_active = self.jobs.iter().any(|job| job.is_active);
        let mut results = Vec::with_capacity(self.jobs.len());
        for job in &mut self.jobs {
            let last_run = database.read_last_run(job.name.as_str()).await?;
            if is_first_run {
                tracing::info!(
                    job = job.name.as_str(),
                    last_run = ?last_run,
                    "scheduler job state loaded"
                );
            }
            let eligible = last_run.is_none_or(|last_run| {
                now_millis >= last_run.saturating_add(job.cadence_millis as i64)
            });
            if !job.enabled {
                results.push(JobCheckResult {
                    name: job.name,
                    config_override: JobConfigOverride::default(),
                    completion_last_run: None,
                    ran: false,
                    skipped: Some(Cow::Borrowed("disabled")),
                });
                continue;
            }
            if job.is_active {
                results.push(JobCheckResult {
                    name: job.name,
                    config_override: JobConfigOverride::default(),
                    completion_last_run: None,
                    ran: false,
                    skipped: Some(Cow::Borrowed("already active")),
                });
                continue;
            }
            if !job.run_ahead_of_schedule {
                if rss_active && job.name != JobName::Rss {
                    results.push(JobCheckResult {
                        name: job.name,
                        config_override: JobConfigOverride::default(),
                        completion_last_run: None,
                        ran: false,
                        skipped: Some(Cow::Borrowed("rss active")),
                    });
                    continue;
                }
                if job.name == JobName::Cleanup && any_active {
                    results.push(JobCheckResult {
                        name: job.name,
                        config_override: JobConfigOverride::default(),
                        completion_last_run: None,
                        ran: false,
                        skipped: Some(Cow::Borrowed("another job active")),
                    });
                    continue;
                }
                if !eligible {
                    results.push(JobCheckResult {
                        name: job.name,
                        config_override: JobConfigOverride::default(),
                        completion_last_run: None,
                        ran: false,
                        skipped: Some(Cow::Borrowed("not due")),
                    });
                    continue;
                }
            }
            let config_override = job.run();
            let completion_last_run = if config_override.is_some() {
                Some(if job.delay_next_run {
                    job.delay_next_run = false;
                    now_millis.saturating_add(job.cadence_millis as i64)
                } else {
                    now_millis
                })
            } else {
                None
            };
            if let Some(completion_last_run) = completion_last_run {
                tracing::debug!(
                    job = job.name.as_str(),
                    completion_last_run,
                    "scheduler job dispatch awaiting completion"
                );
            }
            results.push(JobCheckResult {
                name: job.name,
                config_override: config_override.unwrap_or_default(),
                completion_last_run,
                ran: config_override.is_some(),
                skipped: config_override
                    .is_none()
                    .then_some(Cow::Borrowed("already active")),
            });
        }
        Ok(results)
    }

    /// Mark a dispatched job body as finished.
    pub fn finish_job(&mut self, name: JobName) {
        if let Some(job) = self.jobs.iter_mut().find(|job| job.name == name) {
            job.finish();
        }
    }
}

fn read_last_run(database: &Database, name: JobName) -> crate::Result<Option<i64>> {
    database.read_last_run(name.as_str())
}

fn scheduler_error(message: impl Into<Cow<'static, str>>) -> SporosError {
    SporosError::Scheduler {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::{DaemonPlan, JobConfigOverride, JobName, ScheduledJob, Scheduler};
    use crate::{
        api::JobResponse,
        config::{ApiIntegrationConfig, RawConfig, RuntimeConfig, TorrentClientConfig},
        persistence::{AsyncDatabase, Database},
    };
    use std::{
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn check_jobs_runs_due_jobs_and_defers_last_run() {
        let root = temp_path("scheduler-due");
        std::fs::create_dir_all(&root).expect("root");
        let database = Database::open_app_dir(&root).expect("database");
        let mut scheduler = Scheduler::new(vec![
            ScheduledJob::new(JobName::Search, 60_000, true),
            ScheduledJob::new(JobName::Cleanup, 86_400_000, true),
        ]);

        let results = scheduler
            .check_jobs(&database, 1_000, true)
            .expect("check jobs");

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|result| result.ran));
        assert_eq!(results[0].completion_last_run, Some(1_000));
        assert_eq!(scheduler.jobs()[0].runs, 1);
        let last_run = database
            .read_last_run(JobName::Search.as_str())
            .expect("last run");
        assert_eq!(last_run, None);
        let _cleanup = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn async_check_jobs_runs_due_jobs_and_defers_last_run() {
        let root = temp_path("scheduler-async-due");
        std::fs::create_dir_all(&root).expect("root");
        let database = AsyncDatabase::open_app_dir(&root).await.expect("database");
        let mut scheduler = Scheduler::new(vec![
            ScheduledJob::new(JobName::Search, 60_000, true),
            ScheduledJob::new(JobName::Cleanup, 86_400_000, true),
        ]);

        let results = scheduler
            .check_jobs_async(&database, 1_000, true)
            .await
            .expect("check jobs");

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|result| result.ran));
        assert_eq!(results[0].completion_last_run, Some(1_000));
        assert_eq!(scheduler.jobs()[0].runs, 1);
        let last_run = database
            .read_last_run(JobName::Search.as_str())
            .await
            .expect("last run");
        assert_eq!(last_run, None);
        database.close().await;
        let _cleanup = std::fs::remove_dir_all(root);
    }

    #[test]
    fn early_run_sets_delay_next_run_for_search() {
        let root = temp_path("scheduler-early");
        std::fs::create_dir_all(&root).expect("root");
        let database = Database::open_app_dir(&root).expect("database");
        let mut scheduler = Scheduler::new(vec![ScheduledJob::new(JobName::Search, 60_000, true)]);

        let response = scheduler
            .request_early_run(
                &database,
                JobName::Search,
                1_000,
                JobConfigOverride::default(),
            )
            .expect("early");
        assert_eq!(
            response,
            JobResponse::Accepted("search: running ahead of schedule".to_owned())
        );
        assert!(scheduler.jobs()[0].run_ahead_of_schedule);
        assert!(scheduler.jobs()[0].delay_next_run);

        let results = scheduler
            .check_jobs(&database, 1_000, false)
            .expect("check");
        assert_eq!(results[0].completion_last_run, Some(61_000));
        let last_run = database
            .read_last_run(JobName::Search.as_str())
            .expect("last run");
        assert_eq!(last_run, None);
        assert!(!scheduler.jobs()[0].delay_next_run);
        let _cleanup = std::fs::remove_dir_all(root);
    }

    #[test]
    fn early_run_preserves_config_override_for_dispatch() {
        let root = temp_path("scheduler-early-override");
        std::fs::create_dir_all(&root).expect("root");
        let database = Database::open_app_dir(&root).expect("database");
        let mut scheduler = Scheduler::new(vec![ScheduledJob::new(JobName::Search, 60_000, true)]);
        let config_override = JobConfigOverride {
            ignore_exclude_recent_search: true,
            ignore_exclude_older: true,
        };

        scheduler
            .request_early_run(&database, JobName::Search, 1_000, config_override)
            .expect("early");
        let results = scheduler
            .check_jobs(&database, 1_000, false)
            .expect("check");

        assert!(results[0].ran);
        assert_eq!(results[0].config_override, config_override);
        assert_eq!(
            scheduler.jobs()[0].config_override,
            JobConfigOverride::default()
        );
        let _cleanup = std::fs::remove_dir_all(root);
    }

    #[test]
    fn request_early_run_rejects_disabled_active_and_queued() {
        let root = temp_path("scheduler-reject");
        std::fs::create_dir_all(&root).expect("root");
        let database = Database::open_app_dir(&root).expect("database");
        database
            .write_last_run(JobName::Rss.as_str(), 10_000)
            .expect("job log");
        let mut active = ScheduledJob::new(JobName::Search, 60_000, true);
        active.is_active = true;
        let mut scheduler = Scheduler::new(vec![
            active,
            ScheduledJob::new(JobName::Rss, 60_000, true),
            ScheduledJob::new(JobName::Cleanup, 60_000, false),
        ]);

        assert!(matches!(
            scheduler
                .request_early_run(
                    &database,
                    JobName::Cleanup,
                    1_000,
                    JobConfigOverride::default(),
                )
                .expect("disabled"),
            JobResponse::Disabled(_)
        ));
        assert!(matches!(
            scheduler
                .request_early_run(
                    &database,
                    JobName::Search,
                    1_000,
                    JobConfigOverride::default(),
                )
                .expect("active"),
            JobResponse::AlreadyRunning(_)
        ));
        assert!(matches!(
            scheduler
                .request_early_run(&database, JobName::Rss, 1_000, JobConfigOverride::default())
                .expect("queued"),
            JobResponse::NotEligible(_)
        ));
        let _cleanup = std::fs::remove_dir_all(root);
    }

    #[test]
    fn check_jobs_honors_rss_and_cleanup_concurrency() {
        let root = temp_path("scheduler-concurrency");
        std::fs::create_dir_all(&root).expect("root");
        let database = Database::open_app_dir(&root).expect("database");
        let mut rss = ScheduledJob::new(JobName::Rss, 60_000, true);
        rss.is_active = true;
        let mut scheduler = Scheduler::new(vec![
            rss,
            ScheduledJob::new(JobName::Search, 60_000, true),
            ScheduledJob::new(JobName::Cleanup, 60_000, true),
        ]);

        let results = scheduler
            .check_jobs(&database, 1_000, false)
            .expect("check");

        assert_eq!(results[1].skipped.as_deref(), Some("rss active"));
        assert_eq!(results[2].skipped.as_deref(), Some("rss active"));
        let _cleanup = std::fs::remove_dir_all(root);
    }

    #[test]
    fn active_flag_spans_dispatched_job_body() {
        let root = temp_path("scheduler-active-window");
        std::fs::create_dir_all(&root).expect("root");
        let database = Database::open_app_dir(&root).expect("database");
        let mut scheduler = Scheduler::new(vec![
            ScheduledJob::new(JobName::Search, 60_000, true),
            ScheduledJob::new(JobName::Cleanup, 60_000, true),
        ]);

        let results = scheduler
            .check_jobs(&database, 1_000, false)
            .expect("check jobs");

        assert!(results[0].ran);
        assert!(scheduler.jobs()[0].is_active);
        assert!(scheduler.jobs()[1].is_active);
        assert!(matches!(
            scheduler
                .request_early_run(
                    &database,
                    JobName::Search,
                    1_000,
                    JobConfigOverride::default(),
                )
                .expect("active"),
            JobResponse::AlreadyRunning(_)
        ));

        scheduler.finish_job(JobName::Search);
        scheduler.finish_job(JobName::Cleanup);
        assert!(!scheduler.jobs()[0].is_active);
        assert!(!scheduler.jobs()[1].is_active);
        let _cleanup = std::fs::remove_dir_all(root);
    }

    #[test]
    fn cleanup_waits_for_active_job_from_previous_check() {
        let root = temp_path("scheduler-cleanup-active");
        std::fs::create_dir_all(&root).expect("root");
        let database = Database::open_app_dir(&root).expect("database");
        let mut search = ScheduledJob::new(JobName::Search, 60_000, true);
        search.is_active = true;
        let mut scheduler = Scheduler::new(vec![
            search,
            ScheduledJob::new(JobName::Cleanup, 60_000, true),
        ]);

        let results = scheduler
            .check_jobs(&database, 1_000, false)
            .expect("check jobs");

        assert_eq!(results[1].skipped.as_deref(), Some("another job active"));
        let _cleanup = std::fs::remove_dir_all(root);
    }

    #[test]
    fn daemon_plan_honors_no_port_and_runs_startup_before_jobs() {
        let root = temp_path("daemon-plan");
        std::fs::create_dir_all(&root).expect("root");
        let database = Database::open_app_dir(&root).expect("database");
        let raw = RawConfig {
            listen_port: Some(None),
            ..RawConfig::default()
        };
        let config = RuntimeConfig::normalize(raw, &root).expect("config");
        let mut plan = DaemonPlan::from_config(&config);
        let mut indexed = false;

        let run = plan
            .run_startup(&database, 1_000, || {
                indexed = true;
                Ok(())
            })
            .expect("run");

        assert!(indexed);
        assert!(run.startup_indexed);
        assert!(!run.serving);
        assert_eq!(run.jobs.len(), 5);
        assert!(
            run.jobs
                .iter()
                .any(|result| result.name == JobName::Cleanup && result.ran)
        );
        let _cleanup = std::fs::remove_dir_all(root);
    }

    #[test]
    fn daemon_plan_reaches_production_workflows_from_service_runtime() {
        let root = temp_path("daemon-plan-workflows");
        let data_dir = root.join("data");
        std::fs::create_dir_all(&data_dir).expect("data dir");
        let raw = RawConfig {
            torznab: vec![ApiIntegrationConfig {
                url: "https://indexer.example/api".to_owned(),
                api_key: "secret".to_owned(),
            }],
            torrent_clients: vec![
                TorrentClientConfig::parse("qbittorrent:http://localhost:8080").expect("client"),
            ],
            action: Some("inject".to_owned()),
            rss_cadence: Some(900_000),
            search_cadence: Some(86_400_000),
            exclude_recent_search: Some(259_200_000),
            exclude_older: Some(518_400_000),
            data_dirs: vec![data_dir.clone()],
            ..RawConfig::default()
        };
        let config = RuntimeConfig::normalize(raw, &root).expect("config");
        let plan = DaemonPlan::from_config(&config);

        assert!(plan.serve_http);
        assert_eq!(
            plan.scheduler
                .jobs()
                .iter()
                .map(|job| (job.name, job.enabled))
                .collect::<Vec<_>>(),
            vec![
                (JobName::Rss, true),
                (JobName::Search, true),
                (JobName::UpdateIndexerCaps, true),
                (JobName::Inject, true),
                (JobName::Cleanup, true),
            ]
        );
        let _cleanup = std::fs::remove_dir_all(root);
    }

    fn temp_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("sporos-{name}-{}-{nanos}", std::process::id()))
    }
}
