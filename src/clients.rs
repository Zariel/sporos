//! Torrent-client adapter boundary and client mutation operations.

use std::{borrow::Cow, collections::BTreeMap, path::PathBuf};

use url::Url;

use crate::{
    SporosError,
    config::TorrentClientConfig,
    domain::{
        ClientLabel, ClientTorrentMetadata, Decision, File, InfoHash, InjectionResult, Metafile,
        Searchee, TorrentClientKind, TorrentClientMetadata,
    },
    search::parsed_name_and_media,
};

/// Normalized torrent-client adapter identity.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClientIdentity {
    /// Base URL from configuration.
    pub url: String,
    /// Metadata shared with searchees and action selection.
    pub metadata: TorrentClientMetadata<'static>,
}

/// Torrent row returned by a client inventory call.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClientTorrent<'a> {
    /// Torrent info hash.
    pub info_hash: InfoHash<'a>,
    /// Client display name.
    pub name: Cow<'a, str>,
    /// Torrent file tree.
    pub files: Vec<File<'a>>,
    /// Client save path.
    pub save_path: Cow<'a, str>,
    /// Optional category or label.
    pub category: Option<ClientLabel<'a>>,
    /// Optional tags or labels.
    pub tags: Vec<ClientLabel<'a>>,
    /// Sanitized tracker hosts.
    pub trackers: Vec<Cow<'a, str>>,
    /// Whether the torrent is complete.
    pub complete: bool,
    /// Whether the client is currently hash-checking it.
    pub checking: bool,
}

impl<'a> ClientTorrent<'a> {
    /// Convert any borrowed storage into owned storage.
    pub fn into_owned(self) -> ClientTorrent<'static> {
        ClientTorrent {
            info_hash: self.info_hash.into_owned(),
            name: Cow::Owned(self.name.into_owned()),
            files: self.files.into_iter().map(File::into_owned).collect(),
            save_path: Cow::Owned(self.save_path.into_owned()),
            category: self.category.map(ClientLabel::into_owned),
            tags: self.tags.into_iter().map(ClientLabel::into_owned).collect(),
            trackers: self
                .trackers
                .into_iter()
                .map(|tracker| Cow::Owned(tracker.into_owned()))
                .collect(),
            complete: self.complete,
            checking: self.checking,
        }
    }
}

/// Result from mapping a client inventory to searchees.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct ClientSearcheeResult {
    /// Searchable torrent-client searchees.
    pub searchees: Vec<Searchee<'static>>,
    /// Torrents skipped because their metadata could not form a valid searchee.
    pub skipped: usize,
}

/// Download-dir lookup options.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub struct DownloadDirOptions {
    /// Require a complete source torrent.
    pub only_completed: bool,
}

/// Injection request options shared by all adapters.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct InjectionOptions {
    /// Destination directory passed to the client when linking or data-dir injection chooses one.
    pub destination_dir: Option<PathBuf>,
    /// Category or label to assign.
    pub category: Option<ClientLabel<'static>>,
    /// Tags or labels to assign.
    pub tags: Vec<ClientLabel<'static>>,
    /// Add paused/stopped before recheck.
    pub paused: bool,
    /// Skip client-side hash checking where the adapter supports it.
    pub skip_checking: bool,
}

/// Resume loop behavior shared by adapters.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub struct ResumeOptions {
    /// Perform one check/resume pass instead of the full background loop.
    pub check_once: bool,
}

/// Torrent bytes ready to inject.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NewTorrent<'a> {
    /// Parsed metadata for matching and policy decisions.
    pub metafile: Metafile<'a>,
    /// Original `.torrent` bytes.
    pub bytes: Cow<'a, [u8]>,
}

/// Error codes used by shared client-selection logic.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ClientErrorCode {
    /// Torrent is not present in the client.
    NotFound,
    /// The selected client is readonly.
    Readonly,
    /// The client cannot safely resolve a complete source.
    TorrentNotComplete,
    /// Adapter or configuration does not support the requested operation.
    Unsupported,
}

/// Common synchronous torrent-client adapter contract.
pub trait TorrentClient {
    /// Static adapter identity.
    fn metadata(&self) -> &TorrentClientMetadata<'_>;

    /// Whether a torrent exists in the client.
    fn is_torrent_in_client(&self, info_hash: &InfoHash<'_>) -> crate::Result<bool>;

    /// Whether a torrent is complete.
    fn is_torrent_complete(&self, info_hash: &InfoHash<'_>) -> crate::Result<bool>;

    /// Whether a torrent is hash-checking.
    fn is_torrent_checking(&self, info_hash: &InfoHash<'_>) -> crate::Result<bool>;

    /// Return the complete client inventory.
    fn get_all_torrents(&self) -> crate::Result<Vec<ClientTorrent<'static>>>;

    /// Map client inventory to searchable searchees.
    fn get_client_searchees(&self) -> crate::Result<ClientSearcheeResult> {
        let mut result = ClientSearcheeResult::default();
        for torrent in self.get_all_torrents()? {
            match client_torrent_to_searchee(self.metadata(), torrent) {
                Some(searchee) => result.searchees.push(searchee),
                None => result.skipped += 1,
            }
        }
        Ok(result)
    }

    /// Resolve the download directory for a torrent.
    fn get_download_dir(
        &self,
        metafile: &Metafile<'_>,
        options: DownloadDirOptions,
    ) -> crate::Result<Result<PathBuf, ClientErrorCode>>;

    /// Return known download directories keyed by info hash.
    fn get_all_download_dirs(&self) -> crate::Result<BTreeMap<String, PathBuf>>;

    /// Add a candidate torrent to the client.
    fn inject(
        &self,
        new_torrent: &NewTorrent<'_>,
        searchee: &Searchee<'_>,
        decision: Decision,
        options: &InjectionOptions,
    ) -> crate::Result<InjectionResult>;

    /// Trigger a hash check.
    fn recheck_torrent(&self, info_hash: &InfoHash<'_>) -> crate::Result<()>;

    /// Resume or start after injection/recheck policy allows it.
    fn resume_injection(
        &self,
        metafile: &Metafile<'_>,
        decision: Decision,
        options: ResumeOptions,
    ) -> crate::Result<()>;

    /// Validate adapter-specific configuration.
    fn validate_config(&self) -> crate::Result<()>;
}

/// Build client identities from config order and URL host/path rules.
pub fn client_identities(configs: &[TorrentClientConfig]) -> crate::Result<Vec<ClientIdentity>> {
    let mut host_counts = BTreeMap::<String, usize>::new();
    let parsed = configs
        .iter()
        .map(|config| {
            let url = Url::parse(&config.url).map_err(|error| {
                client_error(format!(
                    "invalid torrent client URL {:?}: {error}",
                    config.url
                ))
            })?;
            let host = url
                .host_str()
                .ok_or_else(|| client_error("torrent client URL must include a host"))?
                .to_owned();
            *host_counts.entry(host.clone()).or_default() += 1;
            Ok((config, url, host))
        })
        .collect::<crate::Result<Vec<_>>>()?;

    parsed
        .into_iter()
        .enumerate()
        .map(|(priority, (config, url, host))| {
            let client_host = if host_counts.get(&host).copied().unwrap_or_default() > 1 {
                format!("{}{}", host, normalized_client_path(url.path()))
            } else {
                host
            };
            Ok(ClientIdentity {
                url: config.url.clone(),
                metadata: TorrentClientMetadata::new(
                    client_host,
                    priority as u16,
                    parse_client_kind(&config.kind)?,
                    config.readonly,
                    config.kind.clone(),
                ),
            })
        })
        .collect()
}

/// Select the writable client that should receive an injection.
pub fn select_injection_client<'a>(
    clients: &'a [&'a dyn TorrentClient],
    searchee: &Searchee<'_>,
) -> crate::Result<Option<&'a dyn TorrentClient>> {
    if clients.len() == 1 {
        let client = clients.first().copied();
        return client
            .map(|client| ensure_writable(client).map(|()| Some(client)))
            .unwrap_or(Ok(None));
    }

    if let Some(host) = searchee.client.as_ref().map(|client| client.host.as_ref()) {
        if let Some(client) = clients
            .iter()
            .copied()
            .find(|client| client.metadata().host.as_ref() == host)
        {
            return ensure_writable(client).map(|()| Some(client));
        }
    }

    clients
        .iter()
        .copied()
        .filter(|client| !client.metadata().readonly)
        .min_by_key(|client| client.metadata().priority)
        .map_or(Ok(None), |client| Ok(Some(client)))
}

/// Convert one client inventory item into a searchable searchee.
pub fn client_torrent_to_searchee(
    metadata: &TorrentClientMetadata<'_>,
    torrent: ClientTorrent<'_>,
) -> Option<Searchee<'static>> {
    let (title, media_type) = parsed_name_and_media(&torrent.name, &torrent.files, None);
    let title = title.into_owned();
    let mut searchee = Searchee::from_files(
        torrent.name.into_owned(),
        title,
        torrent
            .files
            .into_iter()
            .map(File::into_owned)
            .collect::<Vec<_>>(),
    );
    searchee.info_hash = Some(torrent.info_hash.into_owned());
    searchee.media_type = media_type;
    searchee.client = Some(ClientTorrentMetadata::new(
        metadata.host.as_ref().to_owned(),
        torrent.save_path.into_owned(),
        torrent.category.map(ClientLabel::into_owned),
        torrent
            .tags
            .into_iter()
            .map(ClientLabel::into_owned)
            .collect(),
        torrent
            .trackers
            .into_iter()
            .map(|tracker| Cow::Owned(tracker.into_owned()))
            .collect(),
    ));
    Some(searchee.into_owned())
}

/// Check whether an adapter can be used as an injection target.
pub fn ensure_writable(client: &dyn TorrentClient) -> crate::Result<()> {
    if client.metadata().readonly {
        Err(client_error(format!(
            "torrent client {} is readonly",
            client.metadata().host
        )))
    } else {
        Ok(())
    }
}

fn parse_client_kind(value: &str) -> crate::Result<TorrentClientKind> {
    match value {
        "qbittorrent" => Ok(TorrentClientKind::QBittorrent),
        "rtorrent" => Ok(TorrentClientKind::RTorrent),
        "transmission" => Ok(TorrentClientKind::Transmission),
        "deluge" => Ok(TorrentClientKind::Deluge),
        _ => Err(client_error(format!("unsupported torrent client: {value}"))),
    }
}

fn normalized_client_path(path: &str) -> String {
    let path = path.trim_end_matches('/');
    if path.is_empty() || path == "/" {
        String::new()
    } else if path.starts_with('/') {
        path.to_owned()
    } else {
        format!("/{path}")
    }
}

fn client_error(message: impl Into<Cow<'static, str>>) -> SporosError {
    SporosError::TorrentClient {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ClientTorrent, DownloadDirOptions, InjectionOptions, NewTorrent, ResumeOptions,
        TorrentClient, client_identities, client_torrent_to_searchee, select_injection_client,
    };
    use crate::{
        config::TorrentClientConfig,
        domain::{
            ClientLabel, Decision, File, InfoHash, InjectionResult, MediaType, Metafile, Searchee,
            TorrentClientKind, TorrentClientMetadata,
        },
    };
    use std::{borrow::Cow, collections::BTreeMap, path::PathBuf};

    #[test]
    fn derives_client_hosts_from_unique_host_or_path() {
        let unique = client_identities(&[
            TorrentClientConfig::parse("qbittorrent:http://qb.example:8080").expect("client"),
            TorrentClientConfig::parse("rtorrent:http://rt.example/RPC2").expect("client"),
        ])
        .expect("identities");

        assert_eq!(unique[0].metadata.host, "qb.example");
        assert_eq!(unique[0].metadata.priority, 0);
        assert_eq!(unique[0].metadata.kind, TorrentClientKind::QBittorrent);
        assert_eq!(unique[1].metadata.host, "rt.example");

        let duplicate = client_identities(&[
            TorrentClientConfig::parse("qbittorrent:http://shared.example/qb").expect("client"),
            TorrentClientConfig::parse("transmission:http://shared.example/transmission")
                .expect("client"),
        ])
        .expect("identities");

        assert_eq!(duplicate[0].metadata.host, "shared.example/qb");
        assert_eq!(duplicate[1].metadata.host, "shared.example/transmission");
    }

    #[test]
    fn maps_client_torrent_to_searchee_metadata() {
        let metadata = TorrentClientMetadata::new(
            "client-a",
            0,
            TorrentClientKind::QBittorrent,
            false,
            "qBittorrent",
        );
        let torrent = ClientTorrent {
            info_hash: InfoHash::from_validated("0123456789abcdef0123456789abcdef01234567"),
            name: Cow::Borrowed("Example.Show.S01E01"),
            files: vec![File::new("Example.Show.S01E01.mkv", 10)],
            save_path: Cow::Borrowed("/downloads"),
            category: Some(ClientLabel::new("tv")),
            tags: vec![ClientLabel::new("tag")],
            trackers: vec![Cow::Borrowed("tracker.example")],
            complete: true,
            checking: false,
        };

        let searchee = client_torrent_to_searchee(&metadata, torrent).expect("searchee");

        assert_eq!(searchee.title, "Example.Show.S01E01");
        assert_eq!(searchee.media_type, MediaType::Episode);
        assert_eq!(
            searchee.client.as_ref().map(|client| client.host.as_ref()),
            Some("client-a")
        );
        assert_eq!(
            searchee
                .client
                .as_ref()
                .and_then(|client| client.category.as_ref())
                .map(ClientLabel::as_str),
            Some("tv")
        );
    }

    #[test]
    fn selects_writable_injection_client_by_rules() {
        let readonly = FakeClient::new("readonly", 0, true);
        let writable = FakeClient::new("writable", 1, false);
        let preferred = FakeClient::new("preferred", 0, false);
        let mut searchee =
            Searchee::from_files("Release", "Release", vec![File::new("file.mkv", 1)]);
        searchee.client = Some(crate::domain::ClientTorrentMetadata::new(
            "preferred",
            "/downloads",
            None,
            Vec::new(),
            Vec::<Cow<'static, str>>::new(),
        ));

        let clients: [&dyn TorrentClient; 3] = [&readonly, &writable, &preferred];
        let selected = select_injection_client(&clients, &searchee)
            .expect("select")
            .expect("client");

        assert_eq!(selected.metadata().host, "preferred");

        let data_source =
            Searchee::from_files("Release", "Release", vec![File::new("file.mkv", 1)]);
        let fallback_clients: [&dyn TorrentClient; 2] = [&readonly, &writable];
        let selected = select_injection_client(&fallback_clients, &data_source)
            .expect("select")
            .expect("client");

        assert_eq!(selected.metadata().host, "writable");
        assert!(select_injection_client(&[&readonly], &data_source).is_err());
    }

    struct FakeClient {
        metadata: TorrentClientMetadata<'static>,
    }

    impl FakeClient {
        fn new(host: &str, priority: u16, readonly: bool) -> Self {
            Self {
                metadata: TorrentClientMetadata::new(
                    host.to_owned(),
                    priority,
                    TorrentClientKind::QBittorrent,
                    readonly,
                    "fake",
                ),
            }
        }
    }

    impl TorrentClient for FakeClient {
        fn metadata(&self) -> &TorrentClientMetadata<'_> {
            &self.metadata
        }

        fn is_torrent_in_client(&self, _info_hash: &InfoHash<'_>) -> crate::Result<bool> {
            Ok(false)
        }

        fn is_torrent_complete(&self, _info_hash: &InfoHash<'_>) -> crate::Result<bool> {
            Ok(false)
        }

        fn is_torrent_checking(&self, _info_hash: &InfoHash<'_>) -> crate::Result<bool> {
            Ok(false)
        }

        fn get_all_torrents(&self) -> crate::Result<Vec<ClientTorrent<'static>>> {
            Ok(Vec::new())
        }

        fn get_download_dir(
            &self,
            _metafile: &Metafile<'_>,
            _options: DownloadDirOptions,
        ) -> crate::Result<Result<PathBuf, super::ClientErrorCode>> {
            Ok(Err(super::ClientErrorCode::NotFound))
        }

        fn get_all_download_dirs(&self) -> crate::Result<BTreeMap<String, PathBuf>> {
            Ok(BTreeMap::new())
        }

        fn inject(
            &self,
            _new_torrent: &NewTorrent<'_>,
            _searchee: &Searchee<'_>,
            _decision: Decision,
            _options: &InjectionOptions,
        ) -> crate::Result<InjectionResult> {
            Ok(InjectionResult::Injected)
        }

        fn recheck_torrent(&self, _info_hash: &InfoHash<'_>) -> crate::Result<()> {
            Ok(())
        }

        fn resume_injection(
            &self,
            _metafile: &Metafile<'_>,
            _decision: Decision,
            _options: ResumeOptions,
        ) -> crate::Result<()> {
            Ok(())
        }

        fn validate_config(&self) -> crate::Result<()> {
            Ok(())
        }
    }
}
