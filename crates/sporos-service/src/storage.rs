use std::collections::HashMap;
use std::fs::{File, OpenOptions, Permissions};
use std::io;
use std::num::NonZeroU32;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

use duroxide::providers::sqlite::{
    SqliteOptions as DuroxideOptions, SqliteProvider, SqliteSynchronous as DuroxideSynchronous,
};
use fs2::FileExt;
use sqlx::SqlitePool;
use sqlx::migrate::Migrator;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use thiserror::Error;

const BUSY_TIMEOUT: Duration = Duration::from_secs(60);
const POOL_CONNECTIONS: NonZeroU32 = NonZeroU32::MIN;
static MIGRATOR: Migrator = sqlx::migrate!("../../migrations");

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectivePragmas {
    pub journal_mode: String,
    pub foreign_keys: bool,
    pub busy_timeout: Duration,
    pub synchronous: String,
}

pub struct Storage {
    pool: SqlitePool,
    effective_pragmas: EffectivePragmas,
    duroxide: std::sync::Arc<SqliteProvider>,
    duroxide_effective_pragmas: EffectivePragmas,
    // Fields drop in declaration order, so ownership remains held until after both pools close.
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
            .max_connections(POOL_CONNECTIONS.get())
            .connect_with(options)
            .await
            .map_err(StorageOpenError::Connect)?;
        let effective_pragmas = inspect_pragmas(&pool).await?;
        run_domain_migrations(&pool)
            .await
            .map_err(StorageOpenError::Migrate)?;

        let database_path = database_path.canonicalize().map_err(|source| {
            StorageOpenError::CanonicalizeDatabase {
                path: database_path.to_owned(),
                source,
            }
        })?;
        let database_url = format!("sqlite://{}", database_path.display());
        let duroxide = std::sync::Arc::new(
            SqliteProvider::new(
                &database_url,
                Some(DuroxideOptions {
                    synchronous: DuroxideSynchronous::Full,
                    max_connections: POOL_CONNECTIONS,
                }),
            )
            .await
            .map_err(StorageOpenError::ConnectDuroxide)?,
        );
        let duroxide_effective_pragmas = inspect_duroxide_pragmas(&duroxide).await?;

        Ok(Self {
            pool,
            effective_pragmas,
            duroxide,
            duroxide_effective_pragmas,
            _ownership_lock: OwnershipLock(lock_file),
        })
    }

    pub fn effective_pragmas(&self) -> &EffectivePragmas {
        &self.effective_pragmas
    }

    pub fn duroxide_effective_pragmas(&self) -> &EffectivePragmas {
        &self.duroxide_effective_pragmas
    }

    pub fn duroxide_provider(&self) -> std::sync::Arc<SqliteProvider> {
        std::sync::Arc::clone(&self.duroxide)
    }

    pub(crate) fn pool(&self) -> &SqlitePool {
        &self.pool
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
    #[error("failed to resolve SQLite database path {path}")]
    CanonicalizeDatabase {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to connect Duroxide to SQLite")]
    ConnectDuroxide(#[source] duroxide_sqlx::Error),
    #[error("failed to inspect SQLite")]
    Inspect(#[source] sqlx::Error),
    #[error("failed to inspect Duroxide SQLite")]
    InspectDuroxide(#[source] duroxide_sqlx::Error),
    #[error("failed to migrate Sporos domain tables")]
    Migrate(#[source] DomainMigrationError),
    #[error("SQLite {name} is {actual}; expected {expected}")]
    UnexpectedPragma {
        name: &'static str,
        expected: &'static str,
        actual: String,
    },
}

#[derive(Debug, Error)]
pub enum DomainMigrationError {
    #[error("database migration operation failed")]
    Database(#[from] sqlx::Error),
    #[error("database contains unknown Sporos migration {0}")]
    UnknownVersion(i64),
    #[error("Sporos migration {0} differs from the applied migration")]
    VersionMismatch(i64),
    #[error("Sporos migration {0} cannot run outside a transaction")]
    NonTransactional(i64),
}

async fn run_domain_migrations(pool: &SqlitePool) -> Result<(), DomainMigrationError> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS sporos_schema_migration (
            version INTEGER PRIMARY KEY,
            description TEXT NOT NULL,
            installed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            checksum BLOB NOT NULL
        ) STRICT
        "#,
    )
    .execute(pool)
    .await?;

    let applied = sqlx::query_as::<_, (i64, Vec<u8>)>(
        "SELECT version, checksum FROM sporos_schema_migration ORDER BY version",
    )
    .fetch_all(pool)
    .await?;
    let known: HashMap<_, _> = MIGRATOR
        .iter()
        .filter(|migration| !migration.migration_type.is_down_migration())
        .map(|migration| (migration.version, migration))
        .collect();

    for (version, checksum) in &applied {
        let migration = known
            .get(version)
            .ok_or(DomainMigrationError::UnknownVersion(*version))?;
        if checksum.as_slice() != migration.checksum.as_ref() {
            return Err(DomainMigrationError::VersionMismatch(*version));
        }
    }

    let applied: std::collections::HashSet<_> =
        applied.into_iter().map(|(version, _)| version).collect();
    for migration in MIGRATOR
        .iter()
        .filter(|migration| !migration.migration_type.is_down_migration())
    {
        if applied.contains(&migration.version) {
            continue;
        }
        if migration.no_tx {
            return Err(DomainMigrationError::NonTransactional(migration.version));
        }

        // Schema and ledger move together so a crash cannot expose a partially applied migration.
        let mut transaction = pool.begin().await?;
        // The SQL is embedded at compile time and its checksum is verified before execution.
        sqlx::raw_sql(sqlx::AssertSqlSafe(migration.sql.as_ref()))
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            r#"
            INSERT INTO sporos_schema_migration (version, description, checksum)
            VALUES (?, ?, ?)
            "#,
        )
        .bind(migration.version)
        .bind(migration.description.as_ref())
        .bind(migration.checksum.as_ref())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
    }

    Ok(())
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

async fn inspect_duroxide_pragmas(
    provider: &SqliteProvider,
) -> Result<EffectivePragmas, StorageOpenError> {
    let pool = provider.get_pool();
    let journal_mode = duroxide_sqlx::query_scalar::<_, String>("PRAGMA journal_mode")
        .fetch_one(pool)
        .await
        .map_err(StorageOpenError::InspectDuroxide)?;
    require_pragma(
        "Duroxide journal_mode",
        "wal",
        journal_mode.eq_ignore_ascii_case("wal"),
        &journal_mode,
    )?;

    let foreign_keys = duroxide_sqlx::query_scalar::<_, i64>("PRAGMA foreign_keys")
        .fetch_one(pool)
        .await
        .map_err(StorageOpenError::InspectDuroxide)?;
    require_pragma(
        "Duroxide foreign_keys",
        "1",
        foreign_keys == 1,
        &foreign_keys.to_string(),
    )?;

    let busy_timeout = duroxide_sqlx::query_scalar::<_, u64>("PRAGMA busy_timeout")
        .fetch_one(pool)
        .await
        .map_err(StorageOpenError::InspectDuroxide)?;
    let expected_busy_timeout = BUSY_TIMEOUT.as_millis() as u64;
    require_pragma(
        "Duroxide busy_timeout",
        "60000",
        busy_timeout == expected_busy_timeout,
        &busy_timeout.to_string(),
    )?;

    let synchronous = duroxide_sqlx::query_scalar::<_, i64>("PRAGMA synchronous")
        .fetch_one(pool)
        .await
        .map_err(StorageOpenError::InspectDuroxide)?;
    require_pragma(
        "Duroxide synchronous",
        "FULL (2)",
        synchronous == 2,
        &synchronous.to_string(),
    )?;

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
        assert_eq!(
            storage.duroxide_effective_pragmas(),
            storage.effective_pragmas()
        );

        assert_eq!(storage.pool.options().get_max_connections(), 1);
        let pool = storage.duroxide.get_pool();
        assert_eq!(pool.options().get_max_connections(), 1);
        let mut connection = pool.acquire().await.expect("acquire Duroxide connection");
        let synchronous = duroxide_sqlx::query_scalar::<_, i64>("PRAGMA synchronous")
            .fetch_one(&mut *connection)
            .await
            .expect("inspect Duroxide connection");
        assert_eq!(synchronous, 2);
    }

    #[tokio::test]
    async fn keeps_migration_ledgers_separate() {
        let directory = TempDir::new().expect("create temporary directory");
        let storage = open_in(&directory).await.expect("open storage");

        let domain_version =
            sqlx::query_scalar::<_, i64>("SELECT version FROM sporos_schema_migration")
                .fetch_one(storage.pool())
                .await
                .expect("read domain migration ledger");
        let duroxide_has_domain_version =
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM _sqlx_migrations WHERE version = 1")
                .fetch_one(storage.pool())
                .await
                .expect("read Duroxide migration ledger");

        assert_eq!(domain_version, 1);
        assert_eq!(duroxide_has_domain_version, 0);
    }

    #[tokio::test]
    async fn rejects_an_unknown_domain_migration() {
        let directory = TempDir::new().expect("create temporary directory");
        let storage = open_in(&directory).await.expect("open storage");
        sqlx::query(
            "INSERT INTO sporos_schema_migration (version, description, checksum) VALUES (999, 'unknown', X'00')",
        )
        .execute(storage.pool())
        .await
        .expect("insert unknown migration");
        drop(storage);

        assert!(matches!(
            open_in(&directory).await,
            Err(StorageOpenError::Migrate(
                DomainMigrationError::UnknownVersion(999)
            ))
        ));
    }

    #[tokio::test]
    async fn rejects_a_changed_domain_migration() {
        let directory = TempDir::new().expect("create temporary directory");
        let storage = open_in(&directory).await.expect("open storage");
        sqlx::query("UPDATE sporos_schema_migration SET checksum = X'00' WHERE version = 1")
            .execute(storage.pool())
            .await
            .expect("change migration checksum");
        drop(storage);

        assert!(matches!(
            open_in(&directory).await,
            Err(StorageOpenError::Migrate(
                DomainMigrationError::VersionMismatch(1)
            ))
        ));
    }

    #[tokio::test]
    async fn rejects_an_unknown_duroxide_migration() {
        let directory = TempDir::new().expect("create temporary directory");
        let storage = open_in(&directory).await.expect("open storage");
        sqlx::query(
            r#"
            INSERT INTO _sqlx_migrations
                (version, description, success, checksum, execution_time)
            VALUES (999, 'unknown', TRUE, X'00', 0)
            "#,
        )
        .execute(storage.pool())
        .await
        .expect("insert unknown Duroxide migration");
        drop(storage);

        let result = open_in(&directory).await;
        let Err(StorageOpenError::ConnectDuroxide(duroxide_sqlx::Error::Migrate(error))) = result
        else {
            panic!("expected an unknown Duroxide migration error");
        };
        assert!(matches!(
            *error,
            duroxide_sqlx::migrate::MigrateError::VersionMissing(999)
        ));
    }

    #[tokio::test]
    async fn rejects_a_duplicate_duroxide_start() {
        let directory = TempDir::new().expect("create temporary directory");
        let storage = open_in(&directory).await.expect("open storage");
        let client = duroxide::Client::new(storage.duroxide_provider());

        client
            .start_orchestration_versioned("task-1", "ProcessCandidate", "1.0.0", "task-1")
            .await
            .expect("reserve first start");
        let duplicate = client
            .start_orchestration_versioned("task-1", "ProcessCandidate", "1.0.0", "task-1")
            .await;

        let Err(duroxide::ClientError::Provider(error)) = duplicate else {
            panic!("expected a duplicate provider error");
        };
        assert!(!error.is_retryable());
        assert!(error.message.contains("UNIQUE constraint"));

        let queued = sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM orchestrator_queue WHERE instance_id = 'task-1'",
        )
        .fetch_one(storage.pool())
        .await
        .expect("count starts");
        assert_eq!(queued, 1);
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
