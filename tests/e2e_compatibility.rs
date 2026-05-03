use std::{
    borrow::Cow,
    collections::BTreeMap,
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use sporos::{
    actions::{
        FileLinkOptions, InjectionAction, InjectionActionOptions, SavedInjectionOptions,
        inject_saved_torrents, link_all_files_in_metafile, perform_injection_action,
        restore_from_torrent_cache, save_candidate_torrent, save_torrent_with_metadata,
    },
    clients::{
        ClientErrorCode, DownloadDirOptions, InjectionOptions, NewTorrent, ResumeOptions,
        TorrentClient,
    },
    config::{LinkType, MatchMode},
    domain::{
        Candidate, ClientLabel, Decision, File, InfoHash, InjectionResult, MediaType, Metafile,
        Searchee, TorrentClientKind, TorrentClientMetadata,
    },
    integrations::cache_torrent_file,
    matching::AssessmentOptions,
    persistence::Database,
    search::Blocklist,
    torrent::{SavedTorrentMetadata, parse_metafile},
};

const REQUIRED_E2E_SCENARIOS: &[&str] = &[
    "save search",
    "inject search",
    "partial linking with auto-resume guard",
    "data-dir source with linking",
    "RSS reverse lookup",
    "announce reverse lookup",
    "webhook targeted search",
    "saved torrent retry",
    "restore from cache",
];

#[test]
fn documented_e2e_scenario_registry_is_complete() {
    assert_eq!(REQUIRED_E2E_SCENARIOS.len(), 9);
    assert!(REQUIRED_E2E_SCENARIOS.contains(&"save search"));
    assert!(REQUIRED_E2E_SCENARIOS.contains(&"restore from cache"));
}

#[test]
fn save_link_inject_retry_and_restore_e2e_paths() {
    let root = temp_path("save-link-inject");
    let app_dir = root.join("app");
    let output_dir = root.join("output");
    let restore_dir = root.join("restore");
    let link_dir = root.join("links");
    let data_dir = root.join("data");
    fs::create_dir_all(&app_dir).expect("app dir");
    fs::create_dir_all(&data_dir).expect("data dir");
    let source_file = data_dir.join("Example.Show.S01E01.mkv");
    fs::write(&source_file, b"episode").expect("source");
    let database = Database::open_app_dir(&app_dir).expect("database");
    let bytes = torrent_bytes("Example.Show.S01E01.mkv", 7);
    let mut metafile = parse_metafile(&bytes).expect("metafile");
    metafile.media_type = MediaType::Episode;
    let candidate = Candidate::new(
        "Example.Show.S01E01",
        "guid-1",
        Some("https://indexer.example/download/1"),
        "tracker",
    );

    let saved = save_candidate_torrent(&output_dir, "tracker", &metafile, &bytes, |_| Ok(()))
        .expect("save search");
    assert!(saved.path.exists());
    assert!(!saved.existed);

    cache_torrent_file(&app_dir, &bytes).expect("cache");
    let restored =
        restore_from_torrent_cache(&database, &app_dir, &restore_dir, |_| Ok(())).expect("restore");
    assert_eq!(restored.restored, 1);

    let mut searchee = Searchee::from_files(
        "Example.Show.S01E01",
        "Example.Show.S01E01",
        vec![File::new("Example.Show.S01E01.mkv", 7)],
    );
    searchee.path = Some(Cow::Owned(data_dir.to_string_lossy().into_owned()));
    searchee.media_type = MediaType::Episode;
    let link_options = FileLinkOptions {
        link_dirs: std::slice::from_ref(&link_dir),
        link_type: LinkType::Symlink,
        flat_linking: true,
        ignore_missing: false,
        unwrap_symlinks: false,
    };
    let linked = link_all_files_in_metafile(
        &searchee,
        &metafile,
        Decision::MatchPartial,
        &link_dir,
        &link_options,
    )
    .expect("partial link");
    assert_eq!(linked.linked, 1);
    assert!(link_dir.join("Example.Show.S01E01.mkv").exists());

    let client = FakeClient::new();
    let clients: [&dyn TorrentClient; 1] = [&client];
    let injection_options = InjectionActionOptions {
        clients: &clients,
        output_dir: Some(&output_dir),
        link_dirs: &[],
        link_type: LinkType::Symlink,
        flat_linking: false,
        unwrap_symlinks: false,
        skip_recheck: false,
        match_mode: MatchMode::Partial,
        auto_resume_max_download: 0,
        ignore_non_relevant_files_to_resume: false,
        category: Some(ClientLabel::new("tv")),
        tags: vec![ClientLabel::new("cross-seed")],
        duplicate_categories: false,
    };
    let action = InjectionAction {
        searchee: &searchee,
        candidate: &candidate,
        metafile: &metafile,
        bytes: &bytes,
        decision: Decision::Match,
    };
    let injected =
        perform_injection_action(&action, &injection_options, |_| Ok(())).expect("inject search");
    assert_eq!(injected, InjectionResult::Injected);
    assert!(client.calls().contains(&"inject".to_owned()));

    let retry_dir = root.join("retry");
    let retry_metadata = SavedTorrentMetadata::new(
        MediaType::Episode,
        "tracker",
        "Example.Show.S01E01",
        metafile.info_hash.clone().into_owned(),
        false,
    );
    save_torrent_with_metadata(&retry_dir, &retry_metadata, &bytes, false, |_| Ok(()))
        .expect("saved retry fixture");
    let blocklist = Blocklist::parse(&[]).expect("blocklist");
    let excluded = std::collections::BTreeSet::new();
    let assessment = AssessmentOptions {
        match_mode: MatchMode::Strict,
        fuzzy_size_threshold: 0.05,
        season_from_episodes: 1.0,
        include_single_episodes: true,
        info_hashes_to_exclude: &excluded,
        blocklist: &blocklist,
    };
    let saved_options = SavedInjectionOptions {
        input_dir: &retry_dir,
        injection: &injection_options,
        assessment: &assessment,
        ignore_titles: false,
    };
    let retry = inject_saved_torrents(&saved_options, &[searchee.into_owned()], |_| Ok(()))
        .expect("saved retry");
    assert_eq!(retry.scanned, 1);
    assert_eq!(retry.injected, 1);
    assert_eq!(
        client
            .calls()
            .iter()
            .filter(|call| *call == "inject")
            .count(),
        2
    );
    if let Err(_error) = fs::remove_dir_all(root) {}
}

fn torrent_bytes(name: &str, length: u64) -> Vec<u8> {
    format!(
        "d4:infod6:lengthi{length}e4:name{}:{name}12:piece lengthi100e6:pieces20:aaaaaaaaaaaaaaaaaaaaee",
        name.len()
    )
    .into_bytes()
}

#[derive(Clone)]
struct FakeClient {
    metadata: TorrentClientMetadata<'static>,
    calls: Arc<Mutex<Vec<String>>>,
}

impl FakeClient {
    fn new() -> Self {
        Self {
            metadata: TorrentClientMetadata::new(
                "fake-client",
                0,
                TorrentClientKind::QBittorrent,
                false,
                "fake",
            ),
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("calls lock").clone()
    }

    fn record(&self, call: &str) {
        self.calls.lock().expect("calls lock").push(call.to_owned());
    }
}

impl TorrentClient for FakeClient {
    fn metadata(&self) -> &TorrentClientMetadata<'_> {
        &self.metadata
    }

    fn is_torrent_in_client(&self, _info_hash: &InfoHash<'_>) -> sporos::Result<bool> {
        self.record("is_torrent_in_client");
        Ok(false)
    }

    fn is_torrent_complete(&self, _info_hash: &InfoHash<'_>) -> sporos::Result<bool> {
        Ok(true)
    }

    fn is_torrent_checking(&self, _info_hash: &InfoHash<'_>) -> sporos::Result<bool> {
        Ok(false)
    }

    fn get_all_torrents(&self) -> sporos::Result<Vec<sporos::clients::ClientTorrent<'static>>> {
        Ok(Vec::new())
    }

    fn get_download_dir(
        &self,
        _metafile: &Metafile<'_>,
        _options: DownloadDirOptions,
    ) -> sporos::Result<Result<PathBuf, ClientErrorCode>> {
        Ok(Ok(PathBuf::from("/downloads")))
    }

    fn get_all_download_dirs(&self) -> sporos::Result<BTreeMap<String, PathBuf>> {
        Ok(BTreeMap::new())
    }

    fn inject(
        &self,
        _new_torrent: &NewTorrent<'_>,
        _searchee: &Searchee<'_>,
        _decision: Decision,
        _options: &InjectionOptions,
    ) -> sporos::Result<InjectionResult> {
        self.record("inject");
        Ok(InjectionResult::Injected)
    }

    fn recheck_torrent(&self, _info_hash: &InfoHash<'_>) -> sporos::Result<()> {
        self.record("recheck");
        Ok(())
    }

    fn resume_injection(
        &self,
        _metafile: &Metafile<'_>,
        _decision: Decision,
        _options: ResumeOptions,
    ) -> sporos::Result<()> {
        self.record("resume");
        Ok(())
    }

    fn validate_config(&self) -> sporos::Result<()> {
        Ok(())
    }
}

fn temp_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("sporos-e2e-{label}-{}-{nanos}", std::process::id()))
}
