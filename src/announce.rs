use std::fmt;

use serde::Deserialize;
use sha1::{Digest, Sha1};

use crate::domain::{
    ByteSize, CandidateGuid, DownloadUrl, InfoHash, ItemTitle, ReasonText, TrackerName,
};
use crate::secrets::{CookieSecret, SanitizedUrl, SecretString, sanitize_url_for_logging};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum AnnounceError {
    EmptyField { field: &'static str },
    InvalidConfig { field: &'static str, reason: String },
    InvalidRetryPolicy { reason: String },
    InvalidState { reason: String },
}

impl fmt::Display for AnnounceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyField { field } => write!(formatter, "{field} must not be empty"),
            Self::InvalidConfig { field, reason } => {
                write!(formatter, "invalid announce config `{field}`: {reason}")
            }
            Self::InvalidRetryPolicy { reason } => {
                write!(formatter, "invalid retry policy: {reason}")
            }
            Self::InvalidState { reason } => write!(formatter, "invalid announce state: {reason}"),
        }
    }
}

impl std::error::Error for AnnounceError {}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct AnnounceWorkId(String);

impl AnnounceWorkId {
    pub fn new(value: impl Into<String>) -> Result<Self, AnnounceError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(AnnounceError::EmptyField { field: "work id" });
        }
        if !value.starts_with("ann_") {
            return Err(AnnounceError::InvalidState {
                reason: "announce work id must use the ann_ prefix".to_owned(),
            });
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AnnounceWorkId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum AnnounceStatus {
    Queued,
    Running,
    Waiting,
    Retryable,
    Succeeded,
    TerminalFailed,
    Expired,
}

impl AnnounceStatus {
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::TerminalFailed | Self::Expired)
    }

    pub const fn is_claimable(self) -> bool {
        matches!(self, Self::Queued | Self::Retryable)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum AnnounceReason {
    Accepted,
    Deduplicated,
    SourceIncomplete,
    InventoryRefreshing,
    DependencyBackoff,
    CandidateDownloading,
    ClientChecking,
    RetryAfter,
    TransientDependencyFailure,
    Saved,
    Injected,
    AlreadyExists,
    NoMatchTerminal,
    InvalidRequest,
    UnsupportedShape,
    UnsafePath,
    InvalidTorrentMetadata,
    Expired,
}

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct AnnounceFetchMaterial {
    download_url: SecretString,
    redacted_download_url: SanitizedUrl,
    cookie: Option<CookieSecret>,
}

impl AnnounceFetchMaterial {
    pub fn new(
        download_url: &DownloadUrl,
        cookie: Option<CookieSecret>,
    ) -> Result<Self, AnnounceError> {
        Ok(Self {
            download_url: SecretString::new("announce.download_url", download_url.as_str())
                .map_err(|error| AnnounceError::InvalidState {
                    reason: error.to_string(),
                })?,
            redacted_download_url: sanitize_url_for_logging(download_url.as_str()),
            cookie,
        })
    }

    pub fn expose_download_url(&self) -> &str {
        self.download_url.expose_secret()
    }

    pub fn redacted_download_url(&self) -> &SanitizedUrl {
        &self.redacted_download_url
    }

    pub fn cookie(&self) -> Option<&CookieSecret> {
        self.cookie.as_ref()
    }
}

impl fmt::Debug for AnnounceFetchMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AnnounceFetchMaterial")
            .field("download_url", &self.redacted_download_url)
            .field("cookie", &self.cookie)
            .finish()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum AnnounceDedupeIdentity {
    InfoHash {
        tracker: TrackerName,
        info_hash: InfoHash,
    },
    Guid {
        tracker: TrackerName,
        guid: CandidateGuid,
    },
    DownloadFingerprint {
        tracker: TrackerName,
        fingerprint: String,
    },
    Fallback {
        tracker: TrackerName,
        title: ItemTitle,
        size: Option<ByteSize>,
        published_at_ms: Option<i64>,
    },
}

impl AnnounceDedupeIdentity {
    pub fn hash(&self) -> AnnounceDedupeHash {
        let mut hasher = Sha1::new();
        match self {
            Self::InfoHash { tracker, info_hash } => {
                update_hash(
                    &mut hasher,
                    ["info_hash", tracker.as_str(), info_hash.as_str()],
                );
            }
            Self::Guid { tracker, guid } => {
                update_hash(&mut hasher, ["guid", tracker.as_str(), guid.as_str()]);
            }
            Self::DownloadFingerprint {
                tracker,
                fingerprint,
            } => {
                update_hash(
                    &mut hasher,
                    ["download", tracker.as_str(), fingerprint.as_str()],
                );
            }
            Self::Fallback {
                tracker,
                title,
                size,
                published_at_ms,
            } => {
                let size = size
                    .map(ByteSize::get)
                    .map_or_else(String::new, |value| value.to_string());
                let published_at_ms =
                    published_at_ms.map_or_else(String::new, |value| value.to_string());
                update_hash(
                    &mut hasher,
                    [
                        "fallback",
                        tracker.as_str(),
                        title.as_str(),
                        &size,
                        &published_at_ms,
                    ],
                );
            }
        }

        AnnounceDedupeHash(hex_digest(hasher.finalize()))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct AnnounceDedupeHash(String);

impl AnnounceDedupeHash {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AnnounceDedupeHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct AnnounceLease {
    pub owner: ReasonText,
    pub lease_until_ms: i64,
}

impl AnnounceLease {
    pub fn new(owner: ReasonText, lease_until_ms: i64, now_ms: i64) -> Result<Self, AnnounceError> {
        if lease_until_ms <= now_ms {
            return Err(AnnounceError::InvalidState {
                reason: "lease deadline must be in the future".to_owned(),
            });
        }

        Ok(Self {
            owner,
            lease_until_ms,
        })
    }
}

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct AnnounceWorkItem {
    pub id: AnnounceWorkId,
    pub status: AnnounceStatus,
    pub reason: AnnounceReason,
    pub dedupe_hash: AnnounceDedupeHash,
    pub title: ItemTitle,
    pub tracker: TrackerName,
    pub guid: Option<CandidateGuid>,
    pub info_hash: Option<InfoHash>,
    pub size: Option<ByteSize>,
    pub fetch: Option<AnnounceFetchMaterial>,
    pub received_at_ms: i64,
    pub updated_at_ms: i64,
    pub first_attempt_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
    pub attempt_count: u32,
    pub next_attempt_at_ms: i64,
    pub expires_at_ms: i64,
    pub lease: Option<AnnounceLease>,
    pub last_dependency_kind: Option<ReasonText>,
    pub last_dependency_name: Option<ReasonText>,
    pub last_error_class: Option<ReasonText>,
    pub last_redacted_message: Option<ReasonText>,
}

impl fmt::Debug for AnnounceWorkItem {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let title = sanitize_url_for_logging(self.title.as_str());
        let tracker = sanitize_url_for_logging(self.tracker.as_str());
        let guid = self
            .guid
            .as_ref()
            .map(|guid| sanitize_url_for_logging(guid.as_str()));
        let last_dependency_kind = self
            .last_dependency_kind
            .as_ref()
            .map(|kind| sanitize_url_for_logging(kind.as_str()));
        let last_dependency_name = self
            .last_dependency_name
            .as_ref()
            .map(|name| sanitize_url_for_logging(name.as_str()));
        let last_error_class = self
            .last_error_class
            .as_ref()
            .map(|class| sanitize_url_for_logging(class.as_str()));
        let last_redacted_message = self
            .last_redacted_message
            .as_ref()
            .map(|message| sanitize_url_for_logging(message.as_str()));

        formatter
            .debug_struct("AnnounceWorkItem")
            .field("id", &self.id)
            .field("status", &self.status)
            .field("reason", &self.reason)
            .field("dedupe_hash", &self.dedupe_hash)
            .field("title", &title)
            .field("tracker", &tracker)
            .field("guid", &guid)
            .field("info_hash", &self.info_hash)
            .field("size", &self.size)
            .field("fetch", &self.fetch)
            .field("received_at_ms", &self.received_at_ms)
            .field("updated_at_ms", &self.updated_at_ms)
            .field("first_attempt_at_ms", &self.first_attempt_at_ms)
            .field("finished_at_ms", &self.finished_at_ms)
            .field("attempt_count", &self.attempt_count)
            .field("next_attempt_at_ms", &self.next_attempt_at_ms)
            .field("expires_at_ms", &self.expires_at_ms)
            .field("lease", &self.lease)
            .field("last_dependency_kind", &last_dependency_kind)
            .field("last_dependency_name", &last_dependency_name)
            .field("last_error_class", &last_error_class)
            .field("last_redacted_message", &last_redacted_message)
            .finish()
    }
}

impl AnnounceWorkItem {
    pub fn validate(&self) -> Result<(), AnnounceError> {
        if self.status.is_terminal() && self.finished_at_ms.is_none() {
            return Err(AnnounceError::InvalidState {
                reason: "terminal announce work must have a finished timestamp".to_owned(),
            });
        }
        if self.status == AnnounceStatus::Running && self.lease.is_none() {
            return Err(AnnounceError::InvalidState {
                reason: "running announce work must have a lease".to_owned(),
            });
        }
        if self.expires_at_ms <= self.received_at_ms {
            return Err(AnnounceError::InvalidState {
                reason: "expiry must be after receipt".to_owned(),
            });
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AnnounceQueueConfig {
    pub max_pending: u32,
    pub worker_concurrency: u16,
    pub claim_batch_size: u16,
    pub lease_duration_secs: u64,
    pub lease_renewal_secs: u64,
    pub default_ttl_secs: u64,
    pub retry_initial_delay_secs: u64,
    pub retry_max_delay_secs: u64,
    pub retry_jitter_ratio: f64,
    pub success_retention_secs: u64,
    pub failure_retention_secs: u64,
    pub remote_candidate_retention_secs: u64,
}

impl Default for AnnounceQueueConfig {
    fn default() -> Self {
        Self {
            max_pending: 1_000,
            worker_concurrency: 2,
            claim_batch_size: 10,
            lease_duration_secs: 300,
            lease_renewal_secs: 120,
            default_ttl_secs: 86_400,
            retry_initial_delay_secs: 30,
            retry_max_delay_secs: 3_600,
            retry_jitter_ratio: 0.2,
            success_retention_secs: 604_800,
            failure_retention_secs: 1_209_600,
            remote_candidate_retention_secs: 2_592_000,
        }
    }
}

impl AnnounceQueueConfig {
    pub fn validate(&self) -> Result<(), AnnounceError> {
        require_nonzero("max_pending", u64::from(self.max_pending))?;
        require_nonzero("worker_concurrency", u64::from(self.worker_concurrency))?;
        require_nonzero("claim_batch_size", u64::from(self.claim_batch_size))?;
        require_nonzero("lease_duration_secs", self.lease_duration_secs)?;
        require_nonzero("lease_renewal_secs", self.lease_renewal_secs)?;
        require_nonzero("default_ttl_secs", self.default_ttl_secs)?;
        require_nonzero("retry_initial_delay_secs", self.retry_initial_delay_secs)?;
        require_nonzero("retry_max_delay_secs", self.retry_max_delay_secs)?;
        require_nonzero("success_retention_secs", self.success_retention_secs)?;
        require_nonzero("failure_retention_secs", self.failure_retention_secs)?;
        require_nonzero(
            "remote_candidate_retention_secs",
            self.remote_candidate_retention_secs,
        )?;

        if u32::from(self.claim_batch_size) > self.max_pending {
            return Err(config_error(
                "claim_batch_size",
                "must not exceed max_pending",
            ));
        }
        if self.lease_renewal_secs >= self.lease_duration_secs {
            return Err(config_error(
                "lease_renewal_secs",
                "must be shorter than lease_duration_secs",
            ));
        }
        if self.retry_initial_delay_secs > self.retry_max_delay_secs {
            return Err(AnnounceError::InvalidRetryPolicy {
                reason: "initial retry delay must not exceed max retry delay".to_owned(),
            });
        }
        if !(0.0..=1.0).contains(&self.retry_jitter_ratio) || !self.retry_jitter_ratio.is_finite() {
            return Err(config_error(
                "retry_jitter_ratio",
                "must be finite and between 0 and 1",
            ));
        }
        if self.default_ttl_secs <= self.retry_max_delay_secs {
            return Err(config_error(
                "default_ttl_secs",
                "must be greater than retry_max_delay_secs",
            ));
        }

        Ok(())
    }
}

fn require_nonzero(field: &'static str, value: u64) -> Result<(), AnnounceError> {
    if value == 0 {
        return Err(config_error(field, "must be greater than zero"));
    }

    Ok(())
}

fn config_error(field: &'static str, reason: impl Into<String>) -> AnnounceError {
    AnnounceError::InvalidConfig {
        field,
        reason: reason.into(),
    }
}

fn update_hash<'a>(hasher: &mut Sha1, parts: impl IntoIterator<Item = &'a str>) {
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
}

fn hex_digest(bytes: impl IntoIterator<Item = u8>) -> String {
    let mut hex = String::with_capacity(40);
    for byte in bytes {
        hex.push_str(&format!("{byte:02x}"));
    }
    hex
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_and_reason_are_separate_state() {
        let identity = AnnounceDedupeIdentity::Guid {
            tracker: TrackerName::new("tracker.example").unwrap(),
            guid: CandidateGuid::new("guid-1").unwrap(),
        };
        let work = AnnounceWorkItem {
            id: AnnounceWorkId::new("ann_01").unwrap(),
            status: AnnounceStatus::Waiting,
            reason: AnnounceReason::SourceIncomplete,
            dedupe_hash: identity.hash(),
            title: ItemTitle::new("Example").unwrap(),
            tracker: TrackerName::new("tracker.example").unwrap(),
            guid: Some(CandidateGuid::new("guid-1").unwrap()),
            info_hash: None,
            size: Some(ByteSize::new(42)),
            fetch: None,
            received_at_ms: 1,
            updated_at_ms: 1,
            first_attempt_at_ms: None,
            finished_at_ms: None,
            attempt_count: 0,
            next_attempt_at_ms: 10,
            expires_at_ms: 100,
            lease: None,
            last_dependency_kind: None,
            last_dependency_name: None,
            last_error_class: None,
            last_redacted_message: None,
        };

        assert_eq!(AnnounceStatus::Waiting, work.status);
        assert_eq!(AnnounceReason::SourceIncomplete, work.reason);
        work.validate().unwrap();
    }

    #[test]
    fn invalid_work_state_is_rejected() {
        let mut work = minimal_work();
        work.status = AnnounceStatus::Running;
        work.lease = None;

        assert_eq!(
            Err(AnnounceError::InvalidState {
                reason: "running announce work must have a lease".to_owned()
            }),
            work.validate()
        );

        work.status = AnnounceStatus::Succeeded;
        work.lease = None;
        work.validate().unwrap_err();
    }

    #[test]
    fn fetch_material_redacts_secret_bearing_fields() {
        let download_url =
            DownloadUrl::new("https://user:pass@indexer.example/download?apikey=secret").unwrap();
        let material = AnnounceFetchMaterial::new(
            &download_url,
            Some(CookieSecret::new("sid=secret").unwrap()),
        )
        .unwrap();
        let debug = format!("{material:?}");

        assert_eq!(download_url.as_str(), material.expose_download_url());
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("secret"));
        assert!(!debug.contains("sid="));
    }

    #[test]
    fn work_item_debug_redacts_fetch_material() {
        let download_url = DownloadUrl::new(
            "https://tracker.example/download?id=1&passkey=secret&torrent_pass=other",
        )
        .unwrap();
        let work = AnnounceWorkItem {
            id: AnnounceWorkId::new("ann_01").unwrap(),
            status: AnnounceStatus::Queued,
            reason: AnnounceReason::Accepted,
            dedupe_hash: AnnounceDedupeIdentity::Guid {
                tracker: TrackerName::new("tracker").unwrap(),
                guid: CandidateGuid::new("https://tracker.example/guid?passkey=guid-secret")
                    .unwrap(),
            }
            .hash(),
            title: ItemTitle::new("https://tracker.example/title?token=title-secret").unwrap(),
            tracker: TrackerName::new("https://tracker.example/api?apikey=tracker-secret").unwrap(),
            guid: Some(
                CandidateGuid::new("https://tracker.example/guid?passkey=guid-secret").unwrap(),
            ),
            info_hash: None,
            size: None,
            fetch: Some(
                AnnounceFetchMaterial::new(
                    &download_url,
                    Some(CookieSecret::new("sid=secret-cookie").unwrap()),
                )
                .unwrap(),
            ),
            received_at_ms: 1,
            updated_at_ms: 1,
            first_attempt_at_ms: None,
            finished_at_ms: None,
            attempt_count: 0,
            next_attempt_at_ms: 1,
            expires_at_ms: 10,
            lease: None,
            last_dependency_kind: Some(
                ReasonText::new("https://tracker.example/kind?token=kind-secret").unwrap(),
            ),
            last_dependency_name: Some(
                ReasonText::new("https://tracker.example/name?token=name-secret").unwrap(),
            ),
            last_error_class: Some(
                ReasonText::new("https://tracker.example/class?token=class-secret").unwrap(),
            ),
            last_redacted_message: Some(
                ReasonText::new("https://tracker.example/message?token=message-secret").unwrap(),
            ),
        };

        let debug = format!("{work:?}");

        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("secret"));
        assert!(!debug.contains("other"));
        assert!(!debug.contains("sid="));
    }

    #[test]
    fn dedupe_hash_prefers_stable_safe_identity() {
        let tracker = TrackerName::new("tracker.example").unwrap();
        let by_hash = AnnounceDedupeIdentity::InfoHash {
            tracker: tracker.clone(),
            info_hash: InfoHash::new("0123456789abcdef0123456789abcdef01234567").unwrap(),
        }
        .hash();
        let by_guid = AnnounceDedupeIdentity::Guid {
            tracker,
            guid: CandidateGuid::new("guid-1").unwrap(),
        }
        .hash();

        assert_ne!(by_hash, by_guid);
        assert_eq!(40, by_hash.as_str().len());
    }

    #[test]
    fn queue_config_validates_retry_and_lease_boundaries() {
        AnnounceQueueConfig::default().validate().unwrap();

        let mut invalid = AnnounceQueueConfig {
            retry_initial_delay_secs: 20,
            retry_max_delay_secs: 10,
            ..AnnounceQueueConfig::default()
        };
        assert!(matches!(
            invalid.validate(),
            Err(AnnounceError::InvalidRetryPolicy { .. })
        ));

        invalid = AnnounceQueueConfig {
            lease_renewal_secs: 300,
            lease_duration_secs: 300,
            ..AnnounceQueueConfig::default()
        };
        assert!(matches!(
            invalid.validate(),
            Err(AnnounceError::InvalidConfig {
                field: "lease_renewal_secs",
                ..
            })
        ));
    }

    fn minimal_work() -> AnnounceWorkItem {
        AnnounceWorkItem {
            id: AnnounceWorkId::new("ann_01").unwrap(),
            status: AnnounceStatus::Queued,
            reason: AnnounceReason::Accepted,
            dedupe_hash: AnnounceDedupeIdentity::Guid {
                tracker: TrackerName::new("tracker.example").unwrap(),
                guid: CandidateGuid::new("guid-1").unwrap(),
            }
            .hash(),
            title: ItemTitle::new("Example").unwrap(),
            tracker: TrackerName::new("tracker.example").unwrap(),
            guid: Some(CandidateGuid::new("guid-1").unwrap()),
            info_hash: None,
            size: None,
            fetch: None,
            received_at_ms: 1,
            updated_at_ms: 1,
            first_attempt_at_ms: None,
            finished_at_ms: None,
            attempt_count: 0,
            next_attempt_at_ms: 1,
            expires_at_ms: 100,
            lease: None,
            last_dependency_kind: None,
            last_dependency_name: None,
            last_error_class: None,
            last_redacted_message: None,
        }
    }
}
