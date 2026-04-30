//! Daemon job scheduling, concurrency rules, and job status tracking.

use std::{borrow::Cow, sync::Mutex};

use rusqlite::{OptionalExtension, params};

use crate::{SporosError, api::JobResponse, persistence::Database};

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
            delay_next_run: false,
            runs: 0,
        }
    }

    fn run(&mut self) -> bool {
        if self.is_active {
            return false;
        }
        self.is_active = true;
        self.runs = self.runs.saturating_add(1);
        self.is_active = false;
        self.run_ahead_of_schedule = false;
        true
    }
}

/// Result from one job considered by `check_jobs`.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct JobCheckResult {
    /// Job name.
    pub name: JobName,
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
                    ran: false,
                    skipped: Some(Cow::Borrowed("disabled")),
                });
                continue;
            }
            if job.is_active {
                results.push(JobCheckResult {
                    name: job.name,
                    ran: false,
                    skipped: Some(Cow::Borrowed("already active")),
                });
                continue;
            }
            if !job.run_ahead_of_schedule {
                if rss_active && job.name != JobName::Rss {
                    results.push(JobCheckResult {
                        name: job.name,
                        ran: false,
                        skipped: Some(Cow::Borrowed("rss active")),
                    });
                    continue;
                }
                if job.name == JobName::Cleanup && any_active {
                    results.push(JobCheckResult {
                        name: job.name,
                        ran: false,
                        skipped: Some(Cow::Borrowed("another job active")),
                    });
                    continue;
                }
                if !eligible {
                    results.push(JobCheckResult {
                        name: job.name,
                        ran: false,
                        skipped: Some(Cow::Borrowed("not due")),
                    });
                    continue;
                }
            }
            let ran = job.run();
            if ran {
                let persisted = if job.delay_next_run {
                    job.delay_next_run = false;
                    now_millis.saturating_add(job.cadence_millis as i64)
                } else {
                    now_millis
                };
                write_last_run(database, job.name, persisted)?;
            }
            results.push(JobCheckResult {
                name: job.name,
                ran,
                skipped: (!ran).then_some(Cow::Borrowed("already active")),
            });
        }
        Ok(results)
    }
}

fn read_last_run(database: &Database, name: JobName) -> crate::Result<Option<i64>> {
    database
        .connection()
        .query_row(
            "SELECT last_run FROM job_log WHERE name = ?1",
            [name.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(persistence_error)
}

fn write_last_run(database: &Database, name: JobName, last_run: i64) -> crate::Result<()> {
    database
        .connection()
        .execute(
            "INSERT INTO job_log (name, last_run)
             VALUES (?1, ?2)
             ON CONFLICT(name) DO UPDATE SET last_run = excluded.last_run",
            params![name.as_str(), last_run],
        )
        .map_err(persistence_error)?;
    Ok(())
}

fn scheduler_error(message: impl Into<Cow<'static, str>>) -> SporosError {
    SporosError::Scheduler {
        message: message.into(),
    }
}

fn persistence_error(error: rusqlite::Error) -> SporosError {
    SporosError::Persistence {
        message: Cow::Owned(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::{JobName, ScheduledJob, Scheduler};
    use crate::{api::JobResponse, persistence::Database};
    use std::{
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn check_jobs_runs_due_jobs_and_persists_last_run() {
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
        assert_eq!(scheduler.jobs()[0].runs, 1);
        let last_run: i64 = database
            .connection()
            .query_row(
                "SELECT last_run FROM job_log WHERE name = 'search'",
                [],
                |row| row.get(0),
            )
            .expect("last run");
        assert_eq!(last_run, 1_000);
        let _cleanup = std::fs::remove_dir_all(root);
    }

    #[test]
    fn early_run_sets_delay_next_run_for_search() {
        let root = temp_path("scheduler-early");
        std::fs::create_dir_all(&root).expect("root");
        let database = Database::open_app_dir(&root).expect("database");
        let mut scheduler = Scheduler::new(vec![ScheduledJob::new(JobName::Search, 60_000, true)]);

        let response = scheduler
            .request_early_run(&database, JobName::Search, 1_000)
            .expect("early");
        assert_eq!(
            response,
            JobResponse::Accepted("search: running ahead of schedule".to_owned())
        );
        assert!(scheduler.jobs()[0].run_ahead_of_schedule);
        assert!(scheduler.jobs()[0].delay_next_run);

        scheduler
            .check_jobs(&database, 1_000, false)
            .expect("check");
        let last_run: i64 = database
            .connection()
            .query_row(
                "SELECT last_run FROM job_log WHERE name = 'search'",
                [],
                |row| row.get(0),
            )
            .expect("last run");
        assert_eq!(last_run, 61_000);
        assert!(!scheduler.jobs()[0].delay_next_run);
        let _cleanup = std::fs::remove_dir_all(root);
    }

    #[test]
    fn request_early_run_rejects_disabled_active_and_queued() {
        let root = temp_path("scheduler-reject");
        std::fs::create_dir_all(&root).expect("root");
        let database = Database::open_app_dir(&root).expect("database");
        database
            .connection()
            .execute(
                "INSERT INTO job_log (name, last_run) VALUES ('rss', 10_000)",
                [],
            )
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
                .request_early_run(&database, JobName::Cleanup, 1_000)
                .expect("disabled"),
            JobResponse::Disabled(_)
        ));
        assert!(matches!(
            scheduler
                .request_early_run(&database, JobName::Search, 1_000)
                .expect("active"),
            JobResponse::AlreadyRunning(_)
        ));
        assert!(matches!(
            scheduler
                .request_early_run(&database, JobName::Rss, 1_000)
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

    fn temp_path(name: &str) -> PathBuf {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("sporos-{name}-{millis}"))
    }
}
