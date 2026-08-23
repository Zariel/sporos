use std::fs::{File, OpenOptions, Permissions};
use std::io;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

use fs2::FileExt;
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use thiserror::Error;

const BUSY_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectivePragmas {
    pub journal_mode: String,
    pub foreign_keys: bool,
    pub busy_timeout: Duration,
    pub synchronous: String,
}

#[derive(Debug)]
pub struct Storage {
    pool: SqlitePool,
    effective_pragmas: EffectivePragmas,
    // Fields drop in declaration order, so ownership remains held until after the pool closes.
    _ownership_lock: OwnershipLock,
}

impl Storage {
    pub async fn open(
        lock_path: impl AsRef<Path>,
        database_path: impl AsRef<Path>,
    ) -> Result<Self, StorageOpenError> {
        let lock_path = lock_path.as_ref();
        let database_path = database_path.as_ref();

        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(lock_path)
            .map_err(|source| StorageOpenError::OpenLock {
                path: lock_path.to_owned(),
                source,
            })?;
        lock_file
            .set_permissions(Permissions::from_mode(0o600))
            .map_err(|source| StorageOpenError::SetPermissions {
                path: lock_path.to_owned(),
                source,
            })?;
        lock_file.try_lock_exclusive().map_err(|source| {
            if source.kind() == io::ErrorKind::WouldBlock {
                StorageOpenError::AlreadyActive {
                    path: lock_path.to_owned(),
                }
            } else {
                StorageOpenError::AcquireLock {
                    path: lock_path.to_owned(),
                    source,
                }
            }
        })?;

        let database_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(database_path)
            .map_err(|source| StorageOpenError::OpenDatabase {
                path: database_path.to_owned(),
                source,
            })?;
        database_file
            .set_permissions(Permissions::from_mode(0o600))
            .map_err(|source| StorageOpenError::SetPermissions {
                path: database_path.to_owned(),
                source,
            })?;
        drop(database_file);

        let options = SqliteConnectOptions::new()
            .filename(database_path)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .foreign_keys(true)
            .busy_timeout(BUSY_TIMEOUT);
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await
            .map_err(StorageOpenError::Connect)?;
        let effective_pragmas = inspect_pragmas(&pool).await?;

        Ok(Self {
            pool,
            effective_pragmas,
            _ownership_lock: OwnershipLock(lock_file),
        })
    }

    pub fn effective_pragmas(&self) -> &EffectivePragmas {
        &self.effective_pragmas
    }
}

#[derive(Debug)]
struct OwnershipLock(File);

impl Drop for OwnershipLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.0);
    }
}

#[derive(Debug, Error)]
pub enum StorageOpenError {
    #[error("failed to open ownership lock {path}")]
    OpenLock {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("another Sporos process owns {path}")]
    AlreadyActive { path: PathBuf },
    #[error("failed to acquire ownership lock {path}")]
    AcquireLock {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to open database {path}")]
    OpenDatabase {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to restrict permissions on {path}")]
    SetPermissions {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to connect to SQLite")]
    Connect(#[source] sqlx::Error),
    #[error("failed to inspect SQLite")]
    Inspect(#[source] sqlx::Error),
    #[error("SQLite {name} is {actual}; expected {expected}")]
    UnexpectedPragma {
        name: &'static str,
        expected: &'static str,
        actual: String,
    },
}

async fn inspect_pragmas(pool: &SqlitePool) -> Result<EffectivePragmas, StorageOpenError> {
    let journal_mode = sqlx::query_scalar::<_, String>("PRAGMA journal_mode")
        .fetch_one(pool)
        .await
        .map_err(StorageOpenError::Inspect)?;
    require_pragma(
        "journal_mode",
        "wal",
        journal_mode.eq_ignore_ascii_case("wal"),
        &journal_mode,
    )?;

    let foreign_keys = sqlx::query_scalar::<_, i64>("PRAGMA foreign_keys")
        .fetch_one(pool)
        .await
        .map_err(StorageOpenError::Inspect)?;
    require_pragma(
        "foreign_keys",
        "1",
        foreign_keys == 1,
        &foreign_keys.to_string(),
    )?;

    let busy_timeout = sqlx::query_scalar::<_, u64>("PRAGMA busy_timeout")
        .fetch_one(pool)
        .await
        .map_err(StorageOpenError::Inspect)?;
    let expected_busy_timeout = BUSY_TIMEOUT.as_millis() as u64;
    require_pragma(
        "busy_timeout",
        "60000",
        busy_timeout == expected_busy_timeout,
        &busy_timeout.to_string(),
    )?;

    let synchronous = sqlx::query_scalar::<_, i64>("PRAGMA synchronous")
        .fetch_one(pool)
        .await
        .map_err(StorageOpenError::Inspect)?;
    require_pragma(
        "synchronous",
        "FULL (2)",
        synchronous == 2,
        &synchronous.to_string(),
    )?;

    let quick_check = sqlx::query_scalar::<_, String>("PRAGMA quick_check")
        .fetch_one(pool)
        .await
        .map_err(StorageOpenError::Inspect)?;
    require_pragma("quick_check", "ok", quick_check == "ok", &quick_check)?;

    Ok(EffectivePragmas {
        journal_mode,
        foreign_keys: foreign_keys == 1,
        busy_timeout: Duration::from_millis(busy_timeout),
        synchronous: "FULL".to_owned(),
    })
}

fn require_pragma(
    name: &'static str,
    expected: &'static str,
    matches: bool,
    actual: &str,
) -> Result<(), StorageOpenError> {
    if matches {
        Ok(())
    } else {
        Err(StorageOpenError::UnexpectedPragma {
            name,
            expected,
            actual: actual.to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::process::Command;

    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn applies_required_pragmas() {
        let directory = TempDir::new().expect("create temporary directory");
        let storage = open_in(&directory).await.expect("open storage");

        assert_eq!(
            storage.effective_pragmas(),
            &EffectivePragmas {
                journal_mode: "wal".to_owned(),
                foreign_keys: true,
                busy_timeout: BUSY_TIMEOUT,
                synchronous: "FULL".to_owned(),
            }
        );
    }

    #[tokio::test]
    async fn rejects_a_second_owner() {
        let directory = TempDir::new().expect("create temporary directory");
        let first = open_in(&directory).await.expect("open first owner");

        let second = open_in(&directory).await;
        assert!(matches!(
            second,
            Err(StorageOpenError::AlreadyActive { .. })
        ));

        drop(first);
        open_in(&directory)
            .await
            .expect("reacquire released ownership");
    }

    #[tokio::test]
    async fn rejects_another_process() {
        let directory = TempDir::new().expect("create temporary directory");
        let _owner = open_in(&directory).await.expect("open first owner");

        let output = Command::new(std::env::current_exe().expect("locate test executable"))
            .args(["--exact", "storage::tests::ownership_probe", "--nocapture"])
            .env("SPOROS_OWNERSHIP_PROBE_DIR", directory.path())
            .output()
            .expect("run ownership probe");

        assert!(
            output.status.success(),
            "ownership probe failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[tokio::test]
    async fn ownership_probe() {
        let Some(directory) = std::env::var_os("SPOROS_OWNERSHIP_PROBE_DIR") else {
            return;
        };

        let directory = PathBuf::from(directory);
        let result = Storage::open(directory.join("sporos.lock"), directory.join("probe.db")).await;
        assert!(matches!(
            result,
            Err(StorageOpenError::AlreadyActive { .. })
        ));
    }

    #[tokio::test]
    async fn restricts_storage_file_permissions() {
        let directory = TempDir::new().expect("create temporary directory");
        open_in(&directory).await.expect("open storage");

        for file_name in ["sporos.lock", "sporos.db"] {
            let mode = directory
                .path()
                .join(file_name)
                .metadata()
                .expect("read storage file metadata")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }
    }

    async fn open_in(directory: &TempDir) -> Result<Storage, StorageOpenError> {
        Storage::open(
            directory.path().join("sporos.lock"),
            directory.path().join("sporos.db"),
        )
        .await
    }
}
