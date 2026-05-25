use std::path::Path;
use std::sync::Arc;

#[cfg(test)]
use sqlx::SqlitePool;
use sqlx::sqlite::SqliteConnectOptions;
#[cfg(test)]
use tokio::sync::Barrier;
use tokio::sync::Mutex;
use tracing::info_span;

use super::schema_setup::sqlite_pool_options;
use super::{INVENTORY_STAGING_POOL_MAX_CONNECTIONS, Repository, db_error};
use crate::errors::DatabaseError;

impl Repository {
    pub async fn connect(path: impl AsRef<Path>) -> Result<Self, DatabaseError> {
        let path = path.as_ref();
        let _span = info_span!("sqlite.connect", database_path = %path.display());
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true);
        let pool = sqlite_pool_options(5)
            .connect_with(options.clone())
            .await
            .map_err(|error| db_error("connect sqlite database", error))?;
        let inventory_staging_pool = sqlite_pool_options(INVENTORY_STAGING_POOL_MAX_CONNECTIONS)
            .connect_with(options)
            .await
            .map_err(|error| db_error("connect sqlite database", error))?;

        let repository = Self {
            pool,
            inventory_staging_pool,
            inventory_commit_lock: Arc::new(Mutex::new(())),
            prowlarr_sync_lock: Arc::new(Mutex::new(())),
            #[cfg(test)]
            announce_insert_barrier: None,
        };
        repository.initialize().await?;
        Ok(repository)
    }

    pub async fn connect_in_memory() -> Result<Self, DatabaseError> {
        let _span = info_span!("sqlite.connect", database_path = ":memory:");
        let pool = sqlite_pool_options(1)
            .connect("sqlite::memory:")
            .await
            .map_err(|error| db_error("connect in-memory sqlite database", error))?;

        let repository = Self {
            inventory_staging_pool: pool.clone(),
            pool,
            inventory_commit_lock: Arc::new(Mutex::new(())),
            prowlarr_sync_lock: Arc::new(Mutex::new(())),
            #[cfg(test)]
            announce_insert_barrier: None,
        };
        repository.initialize().await?;
        Ok(repository)
    }

    #[cfg(test)]
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    #[cfg(test)]
    pub(super) fn with_announce_insert_barrier(mut self, barrier: Arc<Barrier>) -> Self {
        self.announce_insert_barrier = Some(barrier);
        self
    }

    #[cfg(not(test))]
    pub(super) async fn wait_before_announce_insert_attempt(&self, _attempt: u8) {}

    #[cfg(test)]
    pub(super) async fn wait_before_announce_insert_attempt(&self, attempt: u8) {
        if attempt == 0
            && let Some(barrier) = &self.announce_insert_barrier
        {
            barrier.wait().await;
        }
    }
}
