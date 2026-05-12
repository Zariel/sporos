use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, RwLock};

use crate::domain::{DependencyName, DependencyState, ReasonText};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum DependencyKind {
    TorrentClient,
    Indexer,
    Arr,
    Notification,
    LocalState,
    Database,
    Worker,
}

impl DependencyKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TorrentClient => "torrent_client",
            Self::Indexer => "indexer",
            Self::Arr => "arr",
            Self::Notification => "notification",
            Self::LocalState => "local_state",
            Self::Database => "database",
            Self::Worker => "worker",
        }
    }
}

impl fmt::Display for DependencyKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DependencyKey {
    pub kind: DependencyKind,
    pub name: DependencyName,
}

impl DependencyKey {
    pub fn new(kind: DependencyKind, name: DependencyName) -> Self {
        Self { kind, name }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DependencyHealthEntry {
    pub key: DependencyKey,
    pub state: DependencyState,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DependencyHealthSnapshot {
    pub entries: Vec<DependencyHealthEntry>,
    pub summaries: BTreeMap<DependencyKind, DependencySummary>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum DependencySummary {
    Unknown,
    Healthy,
    Degraded,
    Unavailable,
}

impl DependencySummary {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Unavailable => "unavailable",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct HealthRegistry {
    inner: Arc<RwLock<BTreeMap<DependencyKey, DependencyState>>>,
}

impl HealthRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_unknown(&self, kind: DependencyKind, name: DependencyName) {
        self.set_state(DependencyKey::new(kind, name), DependencyState::Unknown);
    }

    pub fn set_healthy(&self, kind: DependencyKind, name: DependencyName, checked_at_ms: i64) {
        self.set_state(
            DependencyKey::new(kind, name),
            DependencyState::Healthy { checked_at_ms },
        );
    }

    pub fn set_degraded(
        &self,
        kind: DependencyKind,
        name: DependencyName,
        reason: ReasonText,
        retry_after_ms: Option<i64>,
    ) {
        self.set_state(
            DependencyKey::new(kind, name),
            DependencyState::Degraded {
                reason,
                retry_after_ms,
            },
        );
    }

    pub fn set_unavailable(
        &self,
        kind: DependencyKind,
        name: DependencyName,
        reason: ReasonText,
        retry_after_ms: Option<i64>,
    ) {
        self.set_state(
            DependencyKey::new(kind, name),
            DependencyState::Unavailable {
                reason,
                retry_after_ms,
            },
        );
    }

    pub fn set_state(&self, key: DependencyKey, state: DependencyState) {
        let mut health = self
            .inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        health.insert(key, state);
    }

    pub fn state(&self, key: &DependencyKey) -> Option<DependencyState> {
        let health = self
            .inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        health.get(key).cloned()
    }

    pub fn snapshot(&self) -> DependencyHealthSnapshot {
        let health = self
            .inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entries = health
            .iter()
            .map(|(key, state)| DependencyHealthEntry {
                key: key.clone(),
                state: state.clone(),
            })
            .collect::<Vec<_>>();
        let mut summaries = BTreeMap::new();
        for entry in &entries {
            let summary = dependency_summary(&entry.state);
            summaries
                .entry(entry.key.kind)
                .and_modify(|current| *current = worst_summary(*current, summary))
                .or_insert(summary);
        }

        DependencyHealthSnapshot { entries, summaries }
    }
}

fn dependency_summary(state: &DependencyState) -> DependencySummary {
    match state {
        DependencyState::Unknown => DependencySummary::Unknown,
        DependencyState::Healthy { .. } => DependencySummary::Healthy,
        DependencyState::Degraded { .. } => DependencySummary::Degraded,
        DependencyState::Unavailable { .. } => DependencySummary::Unavailable,
    }
}

fn worst_summary(left: DependencySummary, right: DependencySummary) -> DependencySummary {
    if severity(left) >= severity(right) {
        left
    } else {
        right
    }
}

const fn severity(summary: DependencySummary) -> u8 {
    match summary {
        DependencySummary::Healthy => 0,
        DependencySummary::Unknown => 1,
        DependencySummary::Degraded => 2,
        DependencySummary::Unavailable => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_tracks_dependency_state_transitions() {
        let registry = HealthRegistry::new();
        let key = DependencyKey::new(
            DependencyKind::Indexer,
            DependencyName::new("torznab-main").unwrap(),
        );

        registry.set_unknown(key.kind, key.name.clone());
        assert_eq!(Some(DependencyState::Unknown), registry.state(&key));

        registry.set_degraded(
            key.kind,
            key.name.clone(),
            ReasonText::new("rate limited").unwrap(),
            Some(500),
        );
        assert!(matches!(
            registry.state(&key),
            Some(DependencyState::Degraded {
                retry_after_ms: Some(500),
                ..
            })
        ));

        registry.set_healthy(key.kind, key.name.clone(), 1_700_000_000_000);
        assert_eq!(
            Some(DependencyState::Healthy {
                checked_at_ms: 1_700_000_000_000
            }),
            registry.state(&key)
        );
    }

    #[test]
    fn snapshot_groups_worst_state_without_external_checks() {
        let registry = HealthRegistry::new();
        registry.set_healthy(
            DependencyKind::TorrentClient,
            DependencyName::new("qbit-a").unwrap(),
            100,
        );
        registry.set_unavailable(
            DependencyKind::TorrentClient,
            DependencyName::new("qbit-b").unwrap(),
            ReasonText::new("unauthorized").unwrap(),
            None,
        );
        registry.set_degraded(
            DependencyKind::Indexer,
            DependencyName::new("torznab").unwrap(),
            ReasonText::new("retry after").unwrap(),
            Some(200),
        );

        let snapshot = registry.snapshot();

        assert_eq!(3, snapshot.entries.len());
        assert_eq!(
            Some(&DependencySummary::Unavailable),
            snapshot.summaries.get(&DependencyKind::TorrentClient)
        );
        assert_eq!(
            Some(&DependencySummary::Degraded),
            snapshot.summaries.get(&DependencyKind::Indexer)
        );
    }
}
