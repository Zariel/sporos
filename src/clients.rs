use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::config::{ConfigTorrentClientKind, TorrentClientConfig};
use crate::domain::{ClientHost, DependencyName, DisplayName, TorrentClientKind};
use crate::errors::TorrentClientError;
use tracing::debug_span;

pub mod qbittorrent;
pub mod rtorrent;
pub(crate) mod runtime;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum TorrentClientOperation {
    Validate,
    ListInventory,
    FetchFiles,
    FetchTrackers,
    Inject,
    Recheck,
    ResumeStart,
    SetCategory,
    SetTags,
    SetLabel,
    SetSavePath,
}

impl TorrentClientOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Validate => "validate",
            Self::ListInventory => "list inventory",
            Self::FetchFiles => "fetch files",
            Self::FetchTrackers => "fetch trackers",
            Self::Inject => "inject",
            Self::Recheck => "recheck",
            Self::ResumeStart => "resume/start",
            Self::SetCategory => "set category",
            Self::SetTags => "set tags",
            Self::SetLabel => "set label",
            Self::SetSavePath => "set save path",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct TorrentClientCapabilities {
    pub can_validate: bool,
    pub can_list_inventory: bool,
    pub can_fetch_files: bool,
    pub can_fetch_trackers: bool,
    pub can_inject: bool,
    pub can_recheck: bool,
    pub can_resume_start: bool,
    pub supports_categories: bool,
    pub supports_tags: bool,
    pub supports_labels: bool,
    pub supports_save_path: bool,
}

impl TorrentClientCapabilities {
    pub const fn for_kind(kind: TorrentClientKind) -> Self {
        match kind {
            TorrentClientKind::Qbittorrent => Self {
                can_validate: true,
                can_list_inventory: true,
                can_fetch_files: true,
                can_fetch_trackers: true,
                can_inject: true,
                can_recheck: true,
                can_resume_start: true,
                supports_categories: true,
                supports_tags: true,
                supports_labels: false,
                supports_save_path: true,
            },
            TorrentClientKind::Rtorrent => Self {
                can_validate: true,
                can_list_inventory: true,
                can_fetch_files: true,
                can_fetch_trackers: true,
                can_inject: true,
                can_recheck: true,
                can_resume_start: true,
                supports_categories: false,
                supports_tags: false,
                supports_labels: true,
                supports_save_path: true,
            },
        }
    }

    pub const fn supports(self, operation: TorrentClientOperation) -> bool {
        match operation {
            TorrentClientOperation::Validate => self.can_validate,
            TorrentClientOperation::ListInventory => self.can_list_inventory,
            TorrentClientOperation::FetchFiles => self.can_fetch_files,
            TorrentClientOperation::FetchTrackers => self.can_fetch_trackers,
            TorrentClientOperation::Inject => self.can_inject,
            TorrentClientOperation::Recheck => self.can_recheck,
            TorrentClientOperation::ResumeStart => self.can_resume_start,
            TorrentClientOperation::SetCategory => self.supports_categories,
            TorrentClientOperation::SetTags => self.supports_tags,
            TorrentClientOperation::SetLabel => self.supports_labels,
            TorrentClientOperation::SetSavePath => self.supports_save_path,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TorrentClientDescriptor {
    pub name: DisplayName,
    pub kind: TorrentClientKind,
    pub host: ClientHost,
    pub url: String,
    pub default_save_path: PathBuf,
    pub readonly: bool,
    pub capabilities: TorrentClientCapabilities,
}

impl TorrentClientDescriptor {
    /// Durable dependency-health name for this client.
    ///
    /// This intentionally follows `client_host`, not the configured display
    /// name, because local inventory rows and announce waits are keyed by the
    /// client host boundary.
    pub fn dependency_name(&self) -> Result<DependencyName, TorrentClientError> {
        DependencyName::new(self.host.as_str()).map_err(|error| TorrentClientError::BadResponse {
            client: self.name.as_str().to_owned(),
            message: format!("invalid torrent client dependency name: {error}"),
        })
    }

    pub fn ensure_supported(
        &self,
        operation: TorrentClientOperation,
    ) -> Result<(), TorrentClientError> {
        if self.capabilities.supports(operation) {
            return Ok(());
        }

        Err(TorrentClientError::UnsupportedCapability {
            client: self.name.as_str().to_owned(),
            capability: operation.as_str().to_owned(),
        })
    }

    pub const fn can_inject(&self) -> bool {
        !self.readonly && self.capabilities.can_inject
    }
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct TorrentClientRegistry {
    clients: BTreeMap<DisplayName, TorrentClientDescriptor>,
}

impl TorrentClientRegistry {
    pub fn from_config(
        config: &BTreeMap<String, TorrentClientConfig>,
    ) -> Result<Self, TorrentClientError> {
        let _span = debug_span!("torrent_client.registry", client_count = config.len());
        let mut host_counts = BTreeMap::<String, usize>::new();
        for client in config.values() {
            let host = url_host(&client.url).ok_or_else(|| TorrentClientError::BadResponse {
                client: client.url.clone(),
                message: "torrent client url must include a host".to_owned(),
            })?;
            let count = host_counts.entry(host).or_default();
            *count += 1;
        }

        let mut clients = BTreeMap::new();
        let mut dependency_names = BTreeMap::<ClientHost, DisplayName>::new();
        for (name, config) in config {
            let _client_span = debug_span!(
                "torrent_client.configure",
                client_name = name.as_str(),
                client_kind = ?config.kind
            );
            let name = DisplayName::new(name).map_err(|error| TorrentClientError::BadResponse {
                client: name.clone(),
                message: error.to_string(),
            })?;
            let kind = TorrentClientKind::from(config.kind);
            let host = client_host(&config.url, &host_counts).ok_or_else(|| {
                TorrentClientError::BadResponse {
                    client: name.as_str().to_owned(),
                    message: "torrent client url must include a host".to_owned(),
                }
            })?;
            if let Some(existing) = dependency_names.insert(host.clone(), name.clone()) {
                return Err(TorrentClientError::BadResponse {
                    client: name.as_str().to_owned(),
                    message: format!(
                        "torrent client dependency name {host} is already used by {existing}"
                    ),
                });
            }
            let descriptor = TorrentClientDescriptor {
                name: name.clone(),
                kind,
                host,
                url: config.url.clone(),
                default_save_path: config.default_save_path.clone(),
                readonly: false,
                capabilities: TorrentClientCapabilities::for_kind(kind),
            };
            clients.insert(name, descriptor);
        }

        Ok(Self { clients })
    }

    pub fn get(&self, name: &DisplayName) -> Option<&TorrentClientDescriptor> {
        self.clients.get(name)
    }

    pub fn iter(&self) -> impl Iterator<Item = &TorrentClientDescriptor> {
        self.clients.values()
    }

    pub fn len(&self) -> usize {
        self.clients.len()
    }

    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }

    pub fn select_injection_client(
        &self,
        preferred_host: Option<&ClientHost>,
    ) -> Option<&TorrentClientDescriptor> {
        if let Some(preferred_host) = preferred_host
            && let Some(client) = self
                .clients
                .values()
                .find(|client| &client.host == preferred_host && client.can_inject())
        {
            return Some(client);
        }

        self.clients.values().find(|client| client.can_inject())
    }
}

impl From<ConfigTorrentClientKind> for TorrentClientKind {
    fn from(value: ConfigTorrentClientKind) -> Self {
        match value {
            ConfigTorrentClientKind::Qbittorrent => Self::Qbittorrent,
            ConfigTorrentClientKind::Rtorrent => Self::Rtorrent,
        }
    }
}

fn client_host(url: &str, host_counts: &BTreeMap<String, usize>) -> Option<ClientHost> {
    let host = url_host(url)?;
    let value = if host_counts.get(&host).copied().unwrap_or_default() > 1 {
        match url_path(url) {
            Some(path) if path != "/" => format!("{host}/{path}"),
            _ => host,
        }
    } else {
        host
    };

    ClientHost::new(value).ok()
}

fn url_host(url: &str) -> Option<String> {
    let rest = url.split_once("://").map_or(url, |(_scheme, rest)| rest);
    let authority = rest.split('/').next().unwrap_or_default();
    let host = authority
        .rsplit_once('@')
        .map_or(authority, |parts| parts.1);
    if host.trim().is_empty() {
        None
    } else {
        Some(host.to_owned())
    }
}

fn url_path(url: &str) -> Option<&str> {
    let rest = url.split_once("://").map_or(url, |(_scheme, rest)| rest);
    let (_authority, path) = rest.split_once('/')?;
    let path = path.strip_suffix('/').unwrap_or(path);
    Some(if path.is_empty() { "/" } else { path })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::config::parse_config;

    #[test]
    fn registry_builds_qbittorrent_and_rtorrent_capabilities() {
        let config = parse_config(
            r#"
            [torrent_clients.qbit_main]
            kind = "qbittorrent"
            url = "http://qbittorrent:8080"
            default_save_path = "/downloads"

            [torrent_clients.rtorrent_archive]
            kind = "rtorrent"
            url = "http://rtorrent:5000/RPC2"
            default_save_path = "/downloads/archive"
            label_field = "custom1"
            "#,
        )
        .unwrap();

        let registry = TorrentClientRegistry::from_config(&config.torrent_clients).unwrap();
        let qbit = registry
            .get(&DisplayName::new("qbit_main").unwrap())
            .unwrap();
        let rtorrent = registry
            .get(&DisplayName::new("rtorrent_archive").unwrap())
            .unwrap();

        assert_eq!(2, registry.len());
        assert_eq!(TorrentClientKind::Qbittorrent, qbit.kind);
        assert_eq!(PathBuf::from("/downloads"), qbit.default_save_path);
        assert!(!qbit.readonly);
        assert!(qbit.can_inject());
        assert!(qbit.capabilities.supports(TorrentClientOperation::SetTags));
        assert!(
            qbit.capabilities
                .supports(TorrentClientOperation::SetCategory)
        );
        assert_eq!(TorrentClientKind::Rtorrent, rtorrent.kind);
        assert_eq!(
            PathBuf::from("/downloads/archive"),
            rtorrent.default_save_path
        );
        assert!(
            rtorrent
                .capabilities
                .supports(TorrentClientOperation::SetLabel)
        );
        assert!(
            rtorrent
                .capabilities
                .supports(TorrentClientOperation::SetSavePath)
        );
        assert!(
            !rtorrent
                .capabilities
                .supports(TorrentClientOperation::SetTags)
        );
    }

    #[test]
    fn unsupported_capabilities_are_typed_client_errors() {
        let descriptor = TorrentClientDescriptor {
            name: DisplayName::new("rtorrent_archive").unwrap(),
            kind: TorrentClientKind::Rtorrent,
            host: ClientHost::new("rtorrent:5000").unwrap(),
            url: "http://rtorrent:5000/RPC2".to_owned(),
            default_save_path: PathBuf::from("/downloads"),
            readonly: false,
            capabilities: TorrentClientCapabilities::for_kind(TorrentClientKind::Rtorrent),
        };

        let error = descriptor
            .ensure_supported(TorrentClientOperation::SetTags)
            .unwrap_err();

        assert_eq!(
            TorrentClientError::UnsupportedCapability {
                client: "rtorrent_archive".to_owned(),
                capability: "set tags".to_owned()
            },
            error
        );
    }

    #[test]
    fn duplicate_hosts_include_path_for_stable_identity() {
        let mut config = BTreeMap::new();
        config.insert(
            "first".to_owned(),
            client_config(
                ConfigTorrentClientKind::Qbittorrent,
                "http://box.local/qbit",
            ),
        );
        config.insert(
            "second".to_owned(),
            client_config(
                ConfigTorrentClientKind::Rtorrent,
                "http://box.local/rtorrent",
            ),
        );

        let registry = TorrentClientRegistry::from_config(&config).unwrap();
        let first = registry.get(&DisplayName::new("first").unwrap()).unwrap();
        let second = registry.get(&DisplayName::new("second").unwrap()).unwrap();

        assert_eq!("box.local/qbit", first.host.as_str());
        assert_eq!("box.local/rtorrent", second.host.as_str());
        assert_eq!("box.local/qbit", first.dependency_name().unwrap().as_str());
        assert_eq!(
            "box.local/rtorrent",
            second.dependency_name().unwrap().as_str()
        );
    }

    #[test]
    fn duplicate_derived_dependency_names_are_rejected() {
        let mut config = BTreeMap::new();
        config.insert(
            "first".to_owned(),
            client_config(
                ConfigTorrentClientKind::Qbittorrent,
                "http://box.local/qbit",
            ),
        );
        config.insert(
            "second".to_owned(),
            client_config(ConfigTorrentClientKind::Rtorrent, "http://box.local/qbit"),
        );

        let error = TorrentClientRegistry::from_config(&config).unwrap_err();

        assert!(error.to_string().contains("dependency name box.local/qbit"));
    }

    #[test]
    fn injection_selection_prefers_matching_writable_host_then_name_order() {
        let mut config = BTreeMap::new();
        config.insert(
            "z_rtorrent".to_owned(),
            client_config(
                ConfigTorrentClientKind::Rtorrent,
                "http://rtorrent:5000/RPC2",
            ),
        );
        config.insert(
            "a_qbit".to_owned(),
            client_config(ConfigTorrentClientKind::Qbittorrent, "http://qbit:8080"),
        );
        let registry = TorrentClientRegistry::from_config(&config).unwrap();

        let preferred = registry
            .select_injection_client(Some(&ClientHost::new("rtorrent:5000").unwrap()))
            .unwrap();
        let fallback = registry.select_injection_client(None).unwrap();

        assert_eq!("z_rtorrent", preferred.name.as_str());
        assert_eq!("a_qbit", fallback.name.as_str());
    }

    #[test]
    fn transmission_and_deluge_are_out_of_initial_config_scope() {
        for kind in ["transmission", "deluge"] {
            let error = parse_config(&format!(
                r#"
                [torrent_clients.unsupported]
                kind = "{kind}"
                url = "http://client"
                default_save_path = "/downloads"
                "#
            ))
            .unwrap_err();

            assert!(error.to_string().contains("unknown variant"));
        }
    }

    fn client_config(kind: ConfigTorrentClientKind, url: &str) -> TorrentClientConfig {
        TorrentClientConfig {
            kind,
            url: url.to_owned(),
            username: None,
            password: None,
            password_file: None,
            password_env: None,
            default_save_path: PathBuf::from("/downloads"),
            default_category: None,
            default_tags: vec![crate::config::DEFAULT_INJECTION_METADATA.to_owned()],
            default_label: crate::config::DEFAULT_INJECTION_METADATA.to_owned(),
            label_field: None,
        }
    }
}
