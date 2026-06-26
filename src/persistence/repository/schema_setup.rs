use std::collections::BTreeSet;

use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Acquire, Executor, Row, SqlitePool};

use super::db_error;
use crate::errors::DatabaseError;
use crate::persistence::schema::CONNECTION_PRAGMAS;

pub(super) async fn reconcile_inline_schema(pool: &SqlitePool) -> Result<(), DatabaseError> {
    if table_exists(pool, "indexers").await? {
        let columns = table_columns(pool, "indexers").await?;
        if !columns.contains("source_kind")
            || !columns.contains("source_name")
            || !columns.contains("source_indexer_id")
            || indexers_has_legacy_url_unique(pool).await?
        {
            rebuild_indexers_table(pool).await?;
        }
    }
    add_column_if_missing(
        pool,
        "dependency_health",
        "failure_count",
        "ALTER TABLE dependency_health ADD COLUMN failure_count INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    if table_exists(pool, "workflow_inventory_waiters").await? {
        for (column, statement) in [
            (
                "lease_owner",
                "ALTER TABLE workflow_inventory_waiters ADD COLUMN lease_owner TEXT",
            ),
            (
                "lease_until_ms",
                "ALTER TABLE workflow_inventory_waiters ADD COLUMN lease_until_ms INTEGER",
            ),
            (
                "attempt_count",
                "ALTER TABLE workflow_inventory_waiters ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0",
            ),
            (
                "last_error",
                "ALTER TABLE workflow_inventory_waiters ADD COLUMN last_error TEXT",
            ),
        ] {
            add_column_if_missing(pool, "workflow_inventory_waiters", column, statement).await?;
        }
    }
    Ok(())
}

async fn table_exists(pool: &SqlitePool, table: &str) -> Result<bool, DatabaseError> {
    let exists: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?")
            .bind(table)
            .fetch_one(pool)
            .await
            .map_err(|error| db_error("inspect sqlite schema", error))?;
    Ok(exists > 0)
}

async fn table_columns(pool: &SqlitePool, table: &str) -> Result<BTreeSet<String>, DatabaseError> {
    let statement = format!("PRAGMA table_info({table})");
    let rows = sqlx::query(&statement)
        .fetch_all(pool)
        .await
        .map_err(|error| db_error("inspect sqlite table columns", error))?;
    Ok(rows
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect())
}

async fn add_column_if_missing(
    pool: &SqlitePool,
    table: &str,
    column: &str,
    statement: &str,
) -> Result<(), DatabaseError> {
    if !table_columns(pool, table).await?.contains(column) {
        pool.execute(statement)
            .await
            .map_err(|error| db_error("reconcile sqlite schema", error))?;
    }
    Ok(())
}

async fn indexers_has_legacy_url_unique(pool: &SqlitePool) -> Result<bool, DatabaseError> {
    let rows = sqlx::query("PRAGMA index_list(indexers)")
        .fetch_all(pool)
        .await
        .map_err(|error| db_error("inspect indexer indexes", error))?;
    for row in rows {
        let unique: i64 = row.get("unique");
        let partial: i64 = row.get("partial");
        if unique == 0 || partial != 0 {
            continue;
        }
        let name: String = row.get("name");
        let statement = format!("PRAGMA index_info({name})");
        let columns = sqlx::query(&statement)
            .fetch_all(pool)
            .await
            .map_err(|error| db_error("inspect indexer index columns", error))?
            .into_iter()
            .map(|row| row.get::<String, _>("name"))
            .collect::<Vec<_>>();
        if columns == ["url"] {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) async fn rebuild_indexers_table(pool: &SqlitePool) -> Result<(), DatabaseError> {
    let legacy_columns = table_columns(pool, "indexers").await?;
    let source_kind = if legacy_columns.contains("source_kind") {
        "COALESCE(source_kind, 'static')"
    } else {
        "'static'"
    };
    let source_name = if legacy_columns.contains("source_name") {
        "COALESCE(source_name, '')"
    } else {
        "''"
    };
    let source_indexer_id = if legacy_columns.contains("source_indexer_id") {
        "COALESCE(NULLIF(source_indexer_id, ''), name)"
    } else {
        "name"
    };
    let insert_statement = format!(
        r#"
        INSERT INTO indexers (
            id,
            name,
            url,
            source_kind,
            source_name,
            source_indexer_id,
            api_key_source,
            enabled,
            capabilities_json,
            state,
            retry_after,
            last_caps_refresh_at,
            created_at,
            updated_at
        )
        SELECT
            id,
            name,
            url,
            {source_kind},
            {source_name},
            {source_indexer_id},
            api_key_source,
            enabled,
            capabilities_json,
            state,
            retry_after,
            last_caps_refresh_at,
            created_at,
            updated_at
        FROM indexers_legacy
        "#
    );
    let mut connection = pool
        .acquire()
        .await
        .map_err(|error| db_error("acquire indexer schema reconciliation connection", error))?;
    sqlx::query("PRAGMA legacy_alter_table = ON")
        .execute(&mut *connection)
        .await
        .map_err(|error| db_error("enable legacy sqlite table rename", error))?;
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .map_err(|error| {
            db_error(
                "disable sqlite foreign keys for schema reconciliation",
                error,
            )
        })?;

    let rebuild_result = async {
        let mut transaction = connection
            .begin()
            .await
            .map_err(|error| db_error("begin indexer schema reconciliation", error))?;
        for statement in [
            "ALTER TABLE indexers RENAME TO indexers_legacy",
            r#"
            CREATE TABLE indexers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                url TEXT NOT NULL,
                source_kind TEXT NOT NULL DEFAULT 'static',
                source_name TEXT NOT NULL DEFAULT '',
                source_indexer_id TEXT NOT NULL DEFAULT '',
                api_key_source TEXT NOT NULL,
                enabled INTEGER NOT NULL,
                capabilities_json TEXT NOT NULL DEFAULT '{}',
                state TEXT NOT NULL,
                retry_after INTEGER,
                last_caps_refresh_at INTEGER,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE (name),
                UNIQUE (source_kind, source_name, source_indexer_id)
            )
            "#,
            insert_statement.as_str(),
            "DROP TABLE indexers_legacy",
        ] {
            sqlx::query(statement)
                .execute(&mut *transaction)
                .await
                .map_err(|error| db_error("reconcile indexer schema", error))?;
        }
        transaction
            .commit()
            .await
            .map_err(|error| db_error("commit indexer schema reconciliation", error))
    }
    .await;
    let restore_foreign_keys = sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&mut *connection)
        .await
        .map_err(|error| db_error("restore sqlite foreign keys", error));
    let restore_legacy_alter_table = sqlx::query("PRAGMA legacy_alter_table = OFF")
        .execute(&mut *connection)
        .await
        .map_err(|error| db_error("restore sqlite table rename behavior", error));

    rebuild_result?;
    restore_foreign_keys?;
    restore_legacy_alter_table?;
    Ok(())
}

pub(super) fn sqlite_pool_options(max_connections: u32) -> SqlitePoolOptions {
    SqlitePoolOptions::new()
        .max_connections(max_connections)
        .after_connect(|connection, _metadata| {
            Box::pin(async move {
                for pragma in CONNECTION_PRAGMAS {
                    connection.execute(*pragma).await?;
                }
                Ok(())
            })
        })
}
