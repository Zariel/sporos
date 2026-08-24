use serde::{Deserialize, Serialize};
use sporos_model::{ReleaseDescriptor, VideoKind};
use sqlx::{QueryBuilder, Row, Sqlite};
use thiserror::Error;

use crate::config::SourceFilters;
use crate::storage::Storage;

const MAX_PLAUSIBLE_ROWS: i64 = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceState {
    Complete,
    Downloading,
}

impl Storage {
    pub async fn preflight_source(
        &self,
        release: &ReleaseDescriptor,
        announced_size: Option<u64>,
        size_tolerance: f64,
        filters: &SourceFilters,
    ) -> Result<Option<SourceState>, PreflightError> {
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT is_complete FROM sporos_qbit_torrent WHERE available = 1 AND normalized_title = ",
        );
        query.push_bind(release.primary_title.as_str());
        push_identity(&mut query, release);
        if let Some(size) = announced_size.filter(|size| *size > 0) {
            let delta = ((size as f64) * size_tolerance).ceil() as u64;
            query.push(" AND total_size BETWEEN ");
            query.push_bind(to_i64(size.saturating_sub(delta))?);
            query.push(" AND ");
            query.push_bind(to_i64(size.saturating_add(delta))?);
        }
        push_filters(&mut query, filters);
        query.push(" ORDER BY is_complete DESC, id LIMIT ");
        query.push_bind(MAX_PLAUSIBLE_ROWS);

        let rows = query.build().fetch_all(self.pool()).await?;
        Ok(rows.first().map(|row| {
            if row.get::<i64, _>("is_complete") == 1 {
                SourceState::Complete
            } else {
                SourceState::Downloading
            }
        }))
    }
}

fn push_identity(query: &mut QueryBuilder<Sqlite>, release: &ReleaseDescriptor) {
    match release.kind {
        VideoKind::Movie | VideoKind::Disc => {
            query.push(" AND video_kind IN ('movie', 'disc', 'unknown_video')");
            optional_equal(query, "release_year", release.year.map(i64::from));
        }
        VideoKind::Episode => {
            query.push(" AND video_kind IN ('episode', 'unknown_video')");
            optional_equal(query, "season", release.season.map(i64::from));
            optional_equal(query, "episode", release.episode.map(i64::from));
        }
        VideoKind::SeasonPack => {
            query.push(" AND video_kind IN ('season_pack', 'unknown_video')");
            optional_equal(query, "season", release.season.map(i64::from));
        }
        VideoKind::DateEpisode => {
            query.push(" AND video_kind IN ('date_episode', 'unknown_video')");
            let date = release
                .air_date
                .map(|date| format!("{:04}-{:02}-{:02}", date.year, date.month, date.day));
            query.push(" AND (air_date IS NULL OR ");
            query.push_bind(date.clone());
            query.push(" IS NULL OR air_date = ");
            query.push_bind(date);
            query.push(")");
        }
        VideoKind::AbsoluteEpisode => {
            query.push(" AND video_kind IN ('absolute_episode', 'unknown_video')");
            optional_equal(
                query,
                "absolute_episode",
                release.absolute_episode.map(i64::from),
            );
        }
        VideoKind::UnknownVideo => {}
    }
}

fn optional_equal(query: &mut QueryBuilder<Sqlite>, column: &str, value: Option<i64>) {
    query.push(" AND (");
    query.push(column);
    query.push(" IS NULL OR ");
    query.push_bind(value);
    query.push(" IS NULL OR ");
    query.push(column);
    query.push(" = ");
    query.push_bind(value);
    query.push(")");
}

fn push_filters(query: &mut QueryBuilder<Sqlite>, filters: &SourceFilters) {
    if !filters.include_categories.is_empty() {
        query.push(" AND category IN (");
        let mut values = query.separated(", ");
        for category in &filters.include_categories {
            values.push_bind(category);
        }
        values.push_unseparated(")");
    }
    if !filters.exclude_categories.is_empty() {
        query.push(" AND category NOT IN (");
        let mut values = query.separated(", ");
        for category in &filters.exclude_categories {
            values.push_bind(category);
        }
        values.push_unseparated(")");
    }
    if !filters.include_tags.is_empty() {
        query.push(" AND EXISTS (SELECT 1 FROM json_each(tags_json) WHERE value IN (");
        let mut values = query.separated(", ");
        for tag in &filters.include_tags {
            values.push_bind(tag);
        }
        values.push_unseparated("))");
    }
    if !filters.exclude_tags.is_empty() {
        query.push(" AND NOT EXISTS (SELECT 1 FROM json_each(tags_json) WHERE value IN (");
        let mut values = query.separated(", ");
        for tag in &filters.exclude_tags {
            values.push_bind(tag);
        }
        values.push_unseparated("))");
    }
    if filters.exclude_sporos_managed {
        query.push(
            " AND NOT EXISTS (
                SELECT 1 FROM json_each(tags_json)
                WHERE value = 'sporos' OR value LIKE 'sporos:%'
             )",
        );
    }
}

fn to_i64(value: u64) -> Result<i64, PreflightError> {
    i64::try_from(value).map_err(|_| PreflightError::SizeRange)
}

#[derive(Debug, Error)]
pub enum PreflightError {
    #[error("preflight inventory query failed")]
    Database(#[from] sqlx::Error),
    #[error("announced size is outside the supported range")]
    SizeRange,
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::inventory::{InventoryChange, InventoryDelta};
    use crate::qbit_projection::InventoryState;
    use sporos_matcher::parse_release;

    #[tokio::test]
    async fn finds_complete_and_downloading_sources_with_filters() {
        let directory = TempDir::new().expect("temporary directory");
        let storage = open(&directory).await;
        project(&storage, "1", "Example.Show.S01E02", 1_000, 0, "tv", "keep").await;
        project(
            &storage,
            "2",
            "Example.Show.S01E03",
            2_000,
            10,
            "tv",
            "keep",
        )
        .await;
        let filters = SourceFilters {
            include_categories: vec!["tv".to_owned()],
            include_tags: vec!["keep".to_owned()],
            ..SourceFilters::default()
        };

        assert_eq!(
            storage
                .preflight_source(
                    &parse_release("Example.Show.S01E02.1080p"),
                    Some(1_010),
                    0.02,
                    &filters,
                )
                .await
                .unwrap(),
            Some(SourceState::Complete)
        );
        assert_eq!(
            storage
                .preflight_source(
                    &parse_release("Example.Show.S01E03.1080p"),
                    None,
                    0.02,
                    &filters,
                )
                .await
                .unwrap(),
            Some(SourceState::Downloading)
        );
        assert_eq!(
            storage
                .preflight_source(
                    &parse_release("Example.Show.S01E04.1080p"),
                    None,
                    0.02,
                    &filters,
                )
                .await
                .unwrap(),
            None
        );
    }

    async fn project(
        storage: &Storage,
        qbit_id: &str,
        name: &str,
        total_size: u64,
        amount_left: u64,
        category: &str,
        tags: &str,
    ) {
        storage
            .project_qbit_batch(
                &[InventoryChange::Upsert {
                    qbit_id: qbit_id.to_owned(),
                    delta: Box::new(InventoryDelta {
                        infohash_v1: Some(format!("{qbit_id:0<40}")),
                        name: Some(name.to_owned()),
                        total_size: Some(total_size),
                        amount_left: Some(amount_left),
                        progress: Some(if amount_left == 0 { 1.0 } else { 0.5 }),
                        state: Some(
                            if amount_left == 0 {
                                "uploading"
                            } else {
                                "downloading"
                            }
                            .to_owned(),
                        ),
                        save_path: Some("/downloads".to_owned()),
                        content_path: Some(format!("/downloads/{name}")),
                        category: Some(category.to_owned()),
                        tags: Some(tags.to_owned()),
                        added_on: Some(1),
                        completion_on: Some(0),
                        ..InventoryDelta::default()
                    }),
                }],
                1,
                false,
                1,
            )
            .await
            .expect("project source");
    }

    async fn open(directory: &TempDir) -> Storage {
        let storage = Storage::open(
            directory.path().join("sporos.lock"),
            directory.path().join("sporos.db"),
        )
        .await
        .expect("open storage");
        let _: InventoryState = storage.qbit_inventory_state().await.unwrap();
        storage
    }
}
