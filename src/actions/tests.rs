use super::{
    FileLinkOptions, InjectionAction, InjectionActionOptions, SavedInjectionOptions,
    best_saved_match, cleanup_created_roots, inject_saved_torrents, link_all_files_in_metafile,
    link_destination_dir, perform_injection_action, restore_from_torrent_cache,
    save_candidate_torrent, save_torrent_with_metadata, select_link_dir,
};
use crate::{
    clients::{
        ClientErrorCode, ClientTorrent, DownloadDirOptions, InjectionOptions, NewTorrent,
        ResumeOptions, TorrentClient,
    },
    config::{LinkType, MatchMode},
    domain::{
        Candidate, ClientLabel, ClientTorrentMetadata, Decision, File, InfoHash, InjectionResult,
        MediaType, Metafile, Searchee, TorrentClientKind, TorrentClientMetadata,
    },
    matching::AssessmentOptions,
    persistence::{Database, SqlValue},
    search::Blocklist,
    torrent::{
        SavedTorrentMetadata, parse_metadata_from_filename, torrent_cache_path, torrent_save_path,
    },
};
use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

#[test]
fn save_action_writes_and_touches_existing_output() {
    let root = temp_path("save-action");
    let output_dir = root.join("out");
    fs::create_dir_all(&output_dir).expect("output dir");
    let bytes = torrent_bytes("Saved.Release", "https://tracker.example/announce", 10);
    let metafile = crate::torrent::parse_metafile(&bytes).expect("metafile");
    let mut notifications = Vec::new();

    let saved = save_candidate_torrent(
        &output_dir,
        "TrackerOne",
        &metafile,
        &bytes,
        |notification| {
            notifications.push(notification.clone());
            Ok(())
        },
    )
    .expect("save");

    assert!(!saved.existed);
    assert!(saved.path.exists());
    let filename = saved
        .path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .expect("filename");
    let parsed = parse_metadata_from_filename(filename).expect("metadata");
    assert_eq!(parsed.tracker, "TrackerOne");
    assert_eq!(parsed.name, "Saved.Release");
    assert!(!parsed.cached);
    assert_eq!(notifications.len(), 1);

    let saved_again =
        save_candidate_torrent(&output_dir, "TrackerOne", &metafile, b"changed", |_| Ok(()))
            .expect("save again");

    assert!(saved_again.existed);
    assert_eq!(fs::read(saved.path).expect("saved bytes"), bytes);
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn restore_from_cache_uses_indexer_tracker_names_and_keeps_cache() {
    let root = temp_path("restore-cache");
    fs::create_dir_all(&root).expect("root");
    let output_dir = root.join("out");
    let database = Database::open_app_dir(&root).expect("database");
    database
        .execute_sql(
            "INSERT INTO indexer (name, url, apikey, trackers, active)
                 VALUES ('TrackerName', 'https://indexer.example/api', 'secret', ?1, 1)",
            &[SqlValue::Text(std::borrow::Cow::Borrowed(
                r#"["tracker.example"]"#,
            ))],
        )
        .expect("indexer");
    let bytes = torrent_bytes("Cached.Release", "https://tracker.example/announce", 20);
    let metafile = crate::torrent::parse_metafile(&bytes).expect("metafile");
    let cache_path = torrent_cache_path(&root, &metafile.info_hash);
    fs::create_dir_all(cache_path.parent().expect("cache parent")).expect("cache dir");
    fs::write(&cache_path, &bytes).expect("cache write");
    let mut notifications = 0;

    let summary = restore_from_torrent_cache(&database, &root, &output_dir, |_| {
        notifications += 1;
        Ok(())
    })
    .expect("restore");

    assert_eq!(summary.scanned, 1);
    assert_eq!(summary.restored, 1);
    assert_eq!(summary.failed, 0);
    assert!(cache_path.exists());
    assert_eq!(notifications, 1);
    let outputs = fs::read_dir(&output_dir)
        .expect("output read")
        .collect::<Result<Vec<_>, _>>()
        .expect("entries");
    assert_eq!(outputs.len(), 1);
    let filename = outputs[0].file_name().into_string().expect("utf8 filename");
    let metadata = parse_metadata_from_filename(&filename).expect("metadata");
    assert_eq!(metadata.tracker, "TrackerName");
    assert!(metadata.cached);
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn explicit_metadata_save_supports_unknown_tracker_fallback() {
    let root = temp_path("metadata-save");
    let hash = InfoHash::from_validated("0123456789abcdef0123456789abcdef01234567");
    let metadata = SavedTorrentMetadata::new(
        MediaType::Unknown,
        "UnknownTracker",
        "Restored.Release",
        hash,
        true,
    );

    let saved =
        save_torrent_with_metadata(&root, &metadata, b"torrent", true, |_| Ok(())).expect("save");

    let filename = saved
        .path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .expect("filename");
    let parsed = parse_metadata_from_filename(filename).expect("metadata");
    assert_eq!(parsed.tracker, "UnknownTracker");
    assert!(parsed.cached);
    let _cleanup = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn saved_torrent_save_refuses_symlink_target() {
    let root = temp_path("metadata-save-symlink");
    fs::create_dir_all(&root).expect("root");
    let target = root.join("outside.torrent");
    fs::write(&target, b"outside").expect("outside");
    let hash = InfoHash::from_validated("0123456789abcdef0123456789abcdef01234567");
    let metadata = SavedTorrentMetadata::new(
        MediaType::Unknown,
        "UnknownTracker",
        "Symlink.Release",
        hash,
        true,
    );
    let path = torrent_save_path(&root, &metadata);
    std::os::unix::fs::symlink(&target, &path).expect("symlink");

    let error = save_torrent_with_metadata(&root, &metadata, b"torrent", true, |_| Ok(()))
        .expect_err("symlink rejected");

    assert!(
        error
            .to_string()
            .contains("refusing to save torrent through symlink")
    );
    assert_eq!(fs::read(&target).expect("target"), b"outside");
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn links_exact_tree_with_hardlinks_and_cleans_roots() {
    let root = temp_path("link-exact");
    let source = root.join("downloads/Release");
    let link_dir = root.join("links");
    fs::create_dir_all(&source).expect("source dir");
    fs::write(source.join("file.mkv"), b"video").expect("source file");
    let mut searchee =
        Searchee::from_files("Release", "Release", vec![File::new("Release/file.mkv", 5)]);
    searchee.client = Some(ClientTorrentMetadata::new(
        "client",
        root.join("downloads").display().to_string(),
        None,
        Vec::new(),
        Vec::<Cow<'static, str>>::new(),
    ));
    let candidate = Metafile::from_files(
        InfoHash::from_validated("0123456789abcdef0123456789abcdef01234567"),
        "Release",
        "Release",
        1,
        vec![File::new("Release/file.mkv", 5)],
    );
    let destination = link_destination_dir(&link_dir, "Tracker/One", false);

    let result = link_all_files_in_metafile(
        &searchee,
        &candidate,
        Decision::Match,
        &destination,
        &FileLinkOptions {
            link_dirs: std::slice::from_ref(&link_dir),
            link_type: LinkType::Hardlink,
            flat_linking: false,
            ignore_missing: false,
            unwrap_symlinks: false,
        },
    )
    .expect("link");

    assert_eq!(result.linked, 1);
    assert!(destination.join("Release/file.mkv").exists());
    assert_eq!(result.created_roots, vec![destination.join("Release")]);
    assert_eq!(
        cleanup_created_roots(&result.created_roots).expect("cleanup"),
        1
    );
    assert!(!destination.join("Release").exists());
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn linking_rejects_candidate_paths_outside_destination() {
    let root = temp_path("link-traversal");
    let data = root.join("data");
    let destination = root.join("links");
    fs::create_dir_all(&data).expect("data dir");
    fs::write(data.join("source.mkv"), b"video").expect("source file");
    let mut searchee = Searchee::from_files("Source", "Source", vec![File::new("source.mkv", 5)]);
    searchee.path = Some(Cow::Owned(data.join("source.mkv").display().to_string()));
    let candidate = Metafile::from_files(
        InfoHash::from_validated("0123456789abcdef0123456789abcdef01234567"),
        "Release",
        "Release",
        1,
        vec![File::new("../escape.mkv", 5)],
    );

    let error = link_all_files_in_metafile(
        &searchee,
        &candidate,
        Decision::MatchSizeOnly,
        &destination,
        &FileLinkOptions {
            link_dirs: std::slice::from_ref(&destination),
            link_type: LinkType::Hardlink,
            flat_linking: false,
            ignore_missing: false,
            unwrap_symlinks: false,
        },
    )
    .expect_err("unsafe destination rejected");

    assert!(error.to_string().contains("unsafe link destination"));
    assert!(!root.join("escape.mkv").exists());
    let _cleanup = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn linking_rejects_destination_parent_symlink_escape() {
    let root = temp_path("link-parent-symlink");
    let data = root.join("data");
    let destination = root.join("links");
    let outside = root.join("outside");
    fs::create_dir_all(&data).expect("data dir");
    fs::create_dir_all(&destination).expect("destination");
    fs::create_dir_all(&outside).expect("outside");
    std::os::unix::fs::symlink(&outside, destination.join("Release")).expect("escape symlink");
    fs::write(data.join("source.mkv"), b"video").expect("source file");
    let mut searchee = Searchee::from_files("Source", "Source", vec![File::new("source.mkv", 5)]);
    searchee.path = Some(Cow::Owned(data.join("source.mkv").display().to_string()));
    let candidate = Metafile::from_files(
        InfoHash::from_validated("0123456789abcdef0123456789abcdef01234567"),
        "Release",
        "Release",
        1,
        vec![File::new("Release/file.mkv", 5)],
    );

    let error = link_all_files_in_metafile(
        &searchee,
        &candidate,
        Decision::MatchSizeOnly,
        &destination,
        &FileLinkOptions {
            link_dirs: std::slice::from_ref(&destination),
            link_type: LinkType::Hardlink,
            flat_linking: false,
            ignore_missing: false,
            unwrap_symlinks: false,
        },
    )
    .expect_err("symlink escape rejected");

    assert!(error.to_string().contains("escapes link root"));
    assert!(!outside.join("file.mkv").exists());
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn linking_rejects_filesystem_root_destination() {
    let root = temp_path("link-root-destination");
    let data = root.join("data");
    fs::create_dir_all(&data).expect("data dir");
    fs::write(data.join("source.mkv"), b"video").expect("source file");
    let mut searchee = Searchee::from_files("Source", "Source", vec![File::new("source.mkv", 5)]);
    searchee.path = Some(Cow::Owned(data.join("source.mkv").display().to_string()));
    let candidate = Metafile::from_files(
        InfoHash::from_validated("0123456789abcdef0123456789abcdef01234567"),
        "Release",
        "Release",
        1,
        vec![File::new("safe.mkv", 5)],
    );
    let destination = PathBuf::from("/");

    let error = link_all_files_in_metafile(
        &searchee,
        &candidate,
        Decision::MatchSizeOnly,
        &destination,
        &FileLinkOptions {
            link_dirs: std::slice::from_ref(&destination),
            link_type: LinkType::Hardlink,
            flat_linking: true,
            ignore_missing: false,
            unwrap_symlinks: false,
        },
    )
    .expect_err("root destination rejected");

    assert!(error.to_string().contains("filesystem root"));
    let _cleanup = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn linking_rejects_link_root_that_stats_as_filesystem_root() {
    let root = temp_path("link-root-symlink");
    let data = root.join("data");
    let link_root = root.join("root-link");
    fs::create_dir_all(&data).expect("data dir");
    std::os::unix::fs::symlink("/", &link_root).expect("root symlink");
    fs::write(data.join("source.mkv"), b"video").expect("source file");
    let mut searchee = Searchee::from_files("Source", "Source", vec![File::new("source.mkv", 5)]);
    searchee.path = Some(Cow::Owned(data.join("source.mkv").display().to_string()));
    let candidate = Metafile::from_files(
        InfoHash::from_validated("0123456789abcdef0123456789abcdef01234567"),
        "Release",
        "Release",
        1,
        vec![File::new("safe.mkv", 5)],
    );

    let error = link_all_files_in_metafile(
        &searchee,
        &candidate,
        Decision::MatchSizeOnly,
        &link_root,
        &FileLinkOptions {
            link_dirs: std::slice::from_ref(&link_root),
            link_type: LinkType::Hardlink,
            flat_linking: true,
            ignore_missing: false,
            unwrap_symlinks: false,
        },
    )
    .expect_err("root symlink rejected");

    assert!(error.to_string().contains("filesystem root"));
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn link_cleanup_ignores_preexisting_destination_roots() {
    let root = temp_path("link-preexisting-root");
    let source = root.join("downloads/Release");
    let link_dir = root.join("links");
    fs::create_dir_all(&source).expect("source dir");
    fs::write(source.join("file.mkv"), b"video").expect("source file");
    let mut searchee =
        Searchee::from_files("Release", "Release", vec![File::new("Release/file.mkv", 5)]);
    searchee.client = Some(ClientTorrentMetadata::new(
        "client",
        root.join("downloads").display().to_string(),
        None,
        Vec::new(),
        Vec::<Cow<'static, str>>::new(),
    ));
    let candidate = Metafile::from_files(
        InfoHash::from_validated("0123456789abcdef0123456789abcdef01234567"),
        "Release",
        "Release",
        1,
        vec![File::new("Release/file.mkv", 5)],
    );
    let destination = link_destination_dir(&link_dir, "Tracker", false);
    fs::create_dir_all(destination.join("Release")).expect("preexisting root");
    fs::write(destination.join("Release/user-file.txt"), b"keep").expect("user file");

    let result = link_all_files_in_metafile(
        &searchee,
        &candidate,
        Decision::Match,
        &destination,
        &FileLinkOptions {
            link_dirs: std::slice::from_ref(&link_dir),
            link_type: LinkType::Hardlink,
            flat_linking: false,
            ignore_missing: false,
            unwrap_symlinks: false,
        },
    )
    .expect("link");

    assert_eq!(result.linked, 1);
    assert!(result.created_roots.is_empty());
    assert_eq!(
        cleanup_created_roots(&result.created_roots).expect("cleanup"),
        0
    );
    assert!(destination.join("Release/user-file.txt").exists());
    assert!(destination.join("Release/file.mkv").exists());
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn greedy_linking_prefers_same_name_and_supports_symlink_fallback() {
    let root = temp_path("link-greedy");
    let data = root.join("data");
    let link_dir = root.join("links");
    fs::create_dir_all(&data).expect("data dir");
    fs::write(data.join("same.mkv"), b"video").expect("same");
    fs::write(data.join("other.mkv"), b"video").expect("other");
    let searchee = Searchee::from_files(
        "Release",
        "Release",
        vec![
            File::new(data.join("other.mkv").display().to_string(), 5),
            File::new(data.join("same.mkv").display().to_string(), 5),
        ],
    );
    let candidate = Metafile::from_files(
        InfoHash::from_validated("fedcba9876543210fedcba9876543210fedcba98"),
        "Candidate",
        "Candidate",
        1,
        vec![File::new("Candidate/same.mkv", 5)],
    );
    let selected = select_link_dir(&data, std::slice::from_ref(&link_dir), LinkType::Symlink)
        .expect("select")
        .expect("link dir");
    let destination = link_destination_dir(&selected, "Tracker", true);

    let result = link_all_files_in_metafile(
        &searchee,
        &candidate,
        Decision::MatchSizeOnly,
        &destination,
        &FileLinkOptions {
            link_dirs: std::slice::from_ref(&link_dir),
            link_type: LinkType::Symlink,
            flat_linking: true,
            ignore_missing: false,
            unwrap_symlinks: true,
        },
    )
    .expect("link");

    assert_eq!(result.linked, 1);
    let linked = destination.join("Candidate/same.mkv");
    assert!(
        fs::symlink_metadata(&linked)
            .expect("link metadata")
            .file_type()
            .is_symlink()
    );
    let target = fs::read_link(linked).expect("read link");
    assert!(target.ends_with("same.mkv"));
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn link_probe_source_uses_source_directory() {
    let root = temp_path("link-probe-source");
    let source = root.join("source");
    let nested = source.join("nested");
    fs::create_dir_all(&nested).expect("source dir");
    fs::write(nested.join("episode.mkv"), b"video").expect("source file");

    let (probe, created) = super::probe_source_path(&source).expect("probe source");

    assert!(!created);
    assert_eq!(probe, nested.join("episode.mkv"));
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn link_probe_temp_source_is_created_in_source_directory() {
    let root = temp_path("link-probe-empty-source");
    let source = root.join("source");
    let link_dir = root.join("links");
    fs::create_dir_all(&source).expect("source dir");
    fs::create_dir_all(&link_dir).expect("link dir");

    let (probe, created) = super::probe_source_path(&source).expect("probe source");

    assert!(created);
    assert_eq!(probe.parent(), Some(source.as_path()));
    assert!(
        probe
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(".cross-seed-link-probe-source-"))
    );
    assert!(probe.exists());
    assert!(!link_dir.join(".cross-seed-link-probe-source").exists());
    fs::remove_file(&probe).expect("cleanup probe");
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn link_probe_does_not_clobber_existing_source_probe() {
    let root = temp_path("link-probe-source-collision");
    let source = root.join("source");
    let existing_probe = source.join(".cross-seed-link-probe-source");
    fs::create_dir_all(&source).expect("source dir");
    fs::write(&existing_probe, b"user data").expect("existing probe");

    let (probe, created) = super::probe_source_path(&source).expect("probe source");

    assert!(!created);
    assert_eq!(probe, existing_probe);
    assert_eq!(
        fs::read(source.join(".cross-seed-link-probe-source")).expect("existing probe"),
        b"user data"
    );
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn link_probe_does_not_remove_existing_probe_destination() {
    let root = temp_path("link-probe-collision");
    let source = root.join("source");
    let link_dir = root.join("links");
    let existing_probe = link_dir.join(".cross-seed-link-probe-dest");
    fs::create_dir_all(&source).expect("source dir");
    fs::create_dir_all(&link_dir).expect("link dir");
    fs::write(source.join("episode.mkv"), b"video").expect("source file");
    fs::write(&existing_probe, b"user data").expect("existing probe");

    let compatible =
        super::probe_link_dir(&source, &link_dir, LinkType::Hardlink).expect("probe link dir");

    assert!(compatible);
    assert_eq!(
        fs::read(&existing_probe).expect("existing probe"),
        b"user data"
    );
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn source_files_unchanged_rejects_same_size_modified_file() {
    let root = temp_path("source-mtime");
    let data = root.join("data");
    fs::create_dir_all(&data).expect("data dir");
    fs::write(data.join("source.mkv"), b"video").expect("source file");
    let mut searchee = Searchee::from_files("Source", "Source", vec![File::new("source.mkv", 5)]);
    searchee.path = Some(Cow::Owned(data.display().to_string()));
    searchee.mtime_millis = Some(0);

    assert!(!super::source_files_unchanged(&searchee));
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn source_files_unchanged_requires_indexed_mtime() {
    let root = temp_path("source-mtime-missing");
    let data = root.join("data");
    fs::create_dir_all(&data).expect("data dir");
    fs::write(data.join("source.mkv"), b"video").expect("source file");
    let mut searchee = Searchee::from_files("Source", "Source", vec![File::new("source.mkv", 5)]);
    searchee.path = Some(Cow::Owned(data.display().to_string()));

    assert!(!super::source_files_unchanged(&searchee));
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn client_source_save_path_requires_existing_download_dir() {
    let root = temp_path("client-download-dir");
    let downloads = root.join("downloads");
    let mut searchee = client_searchee("client");
    let mut client = FakeClient::new("client");
    client.download_dir = Ok(downloads.clone());
    let clients: [&dyn TorrentClient; 1] = [&client];

    let missing = super::source_save_path(&searchee, &clients, true).expect("save path");
    assert_eq!(missing, Err(ClientErrorCode::NotFound));

    fs::create_dir_all(&downloads).expect("downloads");
    let existing = super::source_save_path(&searchee, &clients, true).expect("save path");
    assert_eq!(existing, Ok(downloads));
    searchee.client = None;
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn injection_action_links_saves_rechecks_and_resumes() {
    let root = temp_path("inject-action");
    let data = root.join("data");
    let link_dir = root.join("links");
    let output_dir = root.join("out");
    fs::create_dir_all(&data).expect("data dir");
    fs::write(data.join("source.mkv"), b"video-data").expect("source");
    let mut searchee = Searchee::from_files(
        "Source.Release",
        "Source.Release",
        vec![File::new("source.mkv", 10)],
    );
    searchee.path = Some(Cow::Owned(data.display().to_string()));
    index_searchee_mtime(&mut searchee, &data.join("source.mkv"));
    let bytes = torrent_bytes("Candidate.Release", "https://tracker.example/announce", 10);
    let metafile = crate::torrent::parse_metafile(&bytes).expect("metafile");
    let candidate = Candidate::new(
        "Candidate.Release",
        "guid",
        Some("https://indexer.example/download"),
        "Tracker/One",
    );
    let client = FakeClient::new("client");
    let clients: [&dyn TorrentClient; 1] = [&client];
    let mut saved = 0;

    let result = perform_injection_action(
        &InjectionAction {
            searchee: &searchee,
            candidate: &candidate,
            metafile: &metafile,
            bytes: &bytes,
            decision: Decision::MatchPartial,
        },
        &InjectionActionOptions {
            clients: &clients,
            output_dir: Some(&output_dir),
            link_dirs: std::slice::from_ref(&link_dir),
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
        },
        |_| {
            saved += 1;
            Ok(())
        },
    )
    .expect("inject action");

    assert_eq!(result, InjectionResult::Injected);
    assert_eq!(saved, 1);
    assert!(link_dir.join("Tracker_One/Candidate.Release").exists());
    let calls = client.calls.lock().expect("calls").clone();
    assert_eq!(calls, vec!["inject", "recheck", "resume"]);
    assert_eq!(
        client
            .last_options
            .lock()
            .expect("options")
            .as_ref()
            .map(|options| options.paused),
        Some(true)
    );
    assert_eq!(
        fs::read_dir(&output_dir)
            .expect("output")
            .collect::<Result<Vec<_>, _>>()
            .expect("entries")
            .len(),
        1
    );
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn linked_data_injection_selects_compatible_client() {
    let root = temp_path("inject-compatible-client");
    let data = root.join("data");
    let link_dir = root.join("links");
    let incompatible_downloads = root.join("incompatible-downloads");
    let compatible_downloads = root.join("compatible-downloads");
    fs::create_dir_all(&data).expect("data dir");
    fs::create_dir_all(&link_dir).expect("link dir");
    fs::create_dir_all(&compatible_downloads).expect("downloads");
    fs::write(&incompatible_downloads, b"not a directory").expect("blocked downloads");
    fs::write(data.join("source.mkv"), b"video-data").expect("source");
    let mut searchee = Searchee::from_files(
        "Source.Release",
        "Source.Release",
        vec![File::new("source.mkv", 10)],
    );
    searchee.path = Some(Cow::Owned(data.display().to_string()));
    index_searchee_mtime(&mut searchee, &data.join("source.mkv"));
    let bytes = torrent_bytes("Candidate.Release", "https://tracker.example/announce", 10);
    let metafile = crate::torrent::parse_metafile(&bytes).expect("metafile");
    let candidate = Candidate::new(
        "Candidate.Release",
        "guid",
        Some("https://indexer.example/download"),
        "Tracker",
    );
    let incompatible = FakeClient::new("incompatible")
        .with_priority(0)
        .with_download_dir("old", incompatible_downloads);
    let compatible = FakeClient::new("compatible")
        .with_priority(1)
        .with_download_dir("old", compatible_downloads);
    let clients: [&dyn TorrentClient; 2] = [&incompatible, &compatible];

    let result = perform_injection_action(
        &InjectionAction {
            searchee: &searchee,
            candidate: &candidate,
            metafile: &metafile,
            bytes: &bytes,
            decision: Decision::Match,
        },
        &InjectionActionOptions {
            clients: &clients,
            output_dir: None,
            link_dirs: std::slice::from_ref(&link_dir),
            link_type: LinkType::Hardlink,
            flat_linking: false,
            unwrap_symlinks: false,
            skip_recheck: true,
            match_mode: MatchMode::Strict,
            auto_resume_max_download: 0,
            ignore_non_relevant_files_to_resume: false,
            category: None,
            tags: Vec::new(),
            duplicate_categories: false,
        },
        |_| Ok(()),
    )
    .expect("inject action");

    assert_eq!(result, InjectionResult::Injected);
    assert!(incompatible.calls.lock().expect("calls").is_empty());
    assert_eq!(
        compatible.calls.lock().expect("calls").clone(),
        vec!["inject"]
    );
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn recheck_policy_matches_documented_cases() {
    let exact = Metafile::from_files(
        InfoHash::from_validated("0123456789abcdef0123456789abcdef01234567"),
        "Exact.Release",
        "Exact.Release",
        16_384,
        vec![File::new("movie.mkv", 10)],
    );
    let disc = Metafile::from_files(
        InfoHash::from_validated("1111111111111111111111111111111111111111"),
        "Disc.Release",
        "Disc.Release",
        16_384,
        vec![File::new("VIDEO_TS/VTS_01_1.VOB", 10)],
    );

    assert!(super::should_recheck(&exact, Decision::Match, false));
    assert!(!super::should_recheck(&exact, Decision::Match, true));
    assert!(super::should_recheck(&exact, Decision::MatchPartial, true));
    assert!(super::should_recheck(&disc, Decision::Match, true));
}

#[test]
fn partial_resume_waits_when_remaining_exceeds_policy() {
    let root = temp_path("partial-resume-policy");
    let data = root.join("data");
    let link_dir = root.join("links");
    fs::create_dir_all(&data).expect("data dir");
    fs::create_dir_all(&link_dir).expect("link dir");
    fs::write(data.join("source.mkv"), b"video-data").expect("source");
    let mut searchee = Searchee::from_files(
        "Source.Release",
        "Source.Release",
        vec![File::new("source.mkv", 10)],
    );
    searchee.path = Some(Cow::Owned(data.display().to_string()));
    index_searchee_mtime(&mut searchee, &data.join("source.mkv"));
    let bytes = torrent_bytes("Candidate.Release", "https://tracker.example/announce", 10);
    let metafile = crate::torrent::parse_metafile(&bytes).expect("metafile");
    let candidate = Candidate::new(
        "Candidate.Release",
        "guid",
        Some("https://indexer.example/download"),
        "Tracker",
    );
    let client = FakeClient::new("client").with_torrent(ClientTorrent {
        info_hash: metafile.info_hash.clone().into_owned(),
        name: Cow::Borrowed("Candidate.Release"),
        files: metafile
            .files
            .iter()
            .cloned()
            .map(File::into_owned)
            .collect(),
        save_path: Cow::Borrowed("/downloads"),
        category: None,
        tags: Vec::new(),
        trackers: Vec::new(),
        complete: false,
        checking: false,
    });
    let clients: [&dyn TorrentClient; 1] = [&client];

    let result = perform_injection_action(
        &InjectionAction {
            searchee: &searchee,
            candidate: &candidate,
            metafile: &metafile,
            bytes: &bytes,
            decision: Decision::MatchPartial,
        },
        &InjectionActionOptions {
            clients: &clients,
            output_dir: None,
            link_dirs: std::slice::from_ref(&link_dir),
            link_type: LinkType::Symlink,
            flat_linking: false,
            unwrap_symlinks: false,
            skip_recheck: true,
            match_mode: MatchMode::Partial,
            auto_resume_max_download: 0,
            ignore_non_relevant_files_to_resume: false,
            category: None,
            tags: Vec::new(),
            duplicate_categories: false,
        },
        |_| Ok(()),
    )
    .expect("inject action");

    assert_eq!(result, InjectionResult::Injected);
    assert_eq!(
        client.calls.lock().expect("calls").clone(),
        vec!["inject", "recheck"]
    );
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn injection_action_saves_incomplete_sources_for_retry() {
    let root = temp_path("inject-incomplete");
    let output_dir = root.join("out");
    let mut searchee = Searchee::from_files(
        "Source.Release",
        "Source.Release",
        vec![File::new("file.mkv", 10)],
    );
    searchee.client = Some(ClientTorrentMetadata::new(
        "client",
        "/downloads",
        None,
        Vec::new(),
        Vec::<Cow<'static, str>>::new(),
    ));
    searchee.info_hash = Some(InfoHash::from_validated(
        "0123456789abcdef0123456789abcdef01234567",
    ));
    let bytes = torrent_bytes("Candidate.Release", "https://tracker.example/announce", 10);
    let metafile = crate::torrent::parse_metafile(&bytes).expect("metafile");
    let candidate = Candidate::new("Candidate.Release", "guid", None::<String>, "Tracker");
    let mut client = FakeClient::new("client");
    client.download_dir = Err(ClientErrorCode::TorrentNotComplete);
    let clients: [&dyn TorrentClient; 1] = [&client];

    let result = perform_injection_action(
        &InjectionAction {
            searchee: &searchee,
            candidate: &candidate,
            metafile: &metafile,
            bytes: &bytes,
            decision: Decision::Match,
        },
        &InjectionActionOptions {
            clients: &clients,
            output_dir: Some(&output_dir),
            link_dirs: &[],
            link_type: LinkType::Symlink,
            flat_linking: false,
            unwrap_symlinks: false,
            skip_recheck: true,
            match_mode: MatchMode::Strict,
            auto_resume_max_download: 0,
            ignore_non_relevant_files_to_resume: false,
            category: None,
            tags: Vec::new(),
            duplicate_categories: false,
        },
        |_| Ok(()),
    )
    .expect("inject action");

    assert_eq!(result, InjectionResult::TorrentNotComplete);
    assert!(client.calls.lock().expect("calls").is_empty());
    assert_eq!(
        fs::read_dir(&output_dir)
            .expect("output")
            .collect::<Result<Vec<_>, _>>()
            .expect("entries")
            .len(),
        1
    );
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn existing_injection_repairs_missing_links_and_rechecks() {
    let root = temp_path("inject-existing-links");
    let data = root.join("data");
    let link_dir = root.join("links");
    let output_dir = root.join("out");
    fs::create_dir_all(&data).expect("data dir");
    fs::write(data.join("source.mkv"), b"video-data").expect("source");
    let mut searchee = Searchee::from_files(
        "Source.Release",
        "Source.Release",
        vec![File::new("source.mkv", 10)],
    );
    searchee.path = Some(Cow::Owned(data.display().to_string()));
    index_searchee_mtime(&mut searchee, &data.join("source.mkv"));
    let bytes = torrent_bytes("Candidate.Release", "https://tracker.example/announce", 10);
    let metafile = crate::torrent::parse_metafile(&bytes).expect("metafile");
    let candidate = Candidate::new(
        "Candidate.Release",
        "guid",
        Some("https://indexer.example/download"),
        "Tracker/One",
    );
    let mut client = FakeClient::new("client");
    client.existing = true;
    let clients: [&dyn TorrentClient; 1] = [&client];

    let result = perform_injection_action(
        &InjectionAction {
            searchee: &searchee,
            candidate: &candidate,
            metafile: &metafile,
            bytes: &bytes,
            decision: Decision::MatchPartial,
        },
        &InjectionActionOptions {
            clients: &clients,
            output_dir: Some(&output_dir),
            link_dirs: std::slice::from_ref(&link_dir),
            link_type: LinkType::Symlink,
            flat_linking: false,
            unwrap_symlinks: false,
            skip_recheck: true,
            match_mode: MatchMode::Partial,
            auto_resume_max_download: 0,
            ignore_non_relevant_files_to_resume: false,
            category: Some(ClientLabel::new("tv")),
            tags: vec![ClientLabel::new("cross-seed")],
            duplicate_categories: false,
        },
        |_| Ok(()),
    )
    .expect("inject action");

    assert_eq!(result, InjectionResult::AlreadyExists);
    assert!(link_dir.join("Tracker_One/Candidate.Release").exists());
    assert_eq!(
        client.calls.lock().expect("calls").clone(),
        vec!["recheck", "resume"]
    );
    assert!(client.last_options.lock().expect("options").is_none());
    assert!(!output_dir.exists());
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn saved_torrent_injection_deletes_successful_retry() {
    let root = temp_path("saved-inject");
    let input_dir = root.join("saved");
    let data = root.join("data");
    fs::create_dir_all(&data).expect("data");
    fs::write(data.join("Candidate.Release"), b"video-data").expect("source");
    let bytes = torrent_bytes("Candidate.Release", "https://tracker.example/announce", 10);
    let metafile = crate::torrent::parse_metafile(&bytes).expect("metafile");
    let saved =
        save_candidate_torrent(&input_dir, "Tracker", &metafile, &bytes, |_| Ok(())).expect("save");
    let mut searchee = Searchee::from_files(
        "Candidate.Release",
        "Candidate.Release",
        vec![File::new("Candidate.Release", 10)],
    );
    searchee.path = Some(Cow::Owned(data.display().to_string()));
    index_searchee_mtime(&mut searchee, &data.join("Candidate.Release"));
    let client = FakeClient::new("client");
    let clients: [&dyn TorrentClient; 1] = [&client];
    let blocklist = Blocklist::parse(&[]).expect("blocklist");
    let excluded = BTreeSet::new();
    let assessment = AssessmentOptions {
        match_mode: MatchMode::Strict,
        fuzzy_size_threshold: 0.05,
        season_from_episodes: 0.75,
        include_single_episodes: true,
        info_hashes_to_exclude: &excluded,
        blocklist: &blocklist,
    };
    let injection = InjectionActionOptions {
        clients: &clients,
        output_dir: Some(&input_dir),
        link_dirs: &[],
        link_type: LinkType::Symlink,
        flat_linking: false,
        unwrap_symlinks: false,
        skip_recheck: true,
        match_mode: MatchMode::Strict,
        auto_resume_max_download: 0,
        ignore_non_relevant_files_to_resume: false,
        category: None,
        tags: vec![ClientLabel::new("cross-seed")],
        duplicate_categories: false,
    };

    let summary = inject_saved_torrents(
        &SavedInjectionOptions {
            input_dir: &input_dir,
            injection: &injection,
            assessment: &assessment,
            ignore_titles: false,
        },
        &[searchee],
        |_| Ok(()),
    )
    .expect("inject saved");

    assert_eq!(summary.scanned, 1);
    assert_eq!(summary.injected, 1);
    assert_eq!(summary.deleted, 1);
    assert!(!saved.path.exists());
    assert_eq!(client.calls.lock().expect("calls").clone(), vec!["inject"]);
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn saved_torrent_injection_deletes_retry_already_in_client() {
    let root = temp_path("saved-inject-existing");
    let input_dir = root.join("saved");
    let data = root.join("data");
    fs::create_dir_all(&data).expect("data");
    fs::write(data.join("Candidate.Release"), b"video-data").expect("source");
    let bytes = torrent_bytes("Candidate.Release", "https://tracker.example/announce", 10);
    let metafile = crate::torrent::parse_metafile(&bytes).expect("metafile");
    let saved =
        save_candidate_torrent(&input_dir, "Tracker", &metafile, &bytes, |_| Ok(())).expect("save");
    let mut searchee = Searchee::from_files(
        "Candidate.Release",
        "Candidate.Release",
        vec![File::new("Candidate.Release", 10)],
    );
    searchee.path = Some(Cow::Owned(data.display().to_string()));
    index_searchee_mtime(&mut searchee, &data.join("Candidate.Release"));
    let mut client = FakeClient::new("client");
    client.existing = true;
    let clients: [&dyn TorrentClient; 1] = [&client];
    let blocklist = Blocklist::parse(&[]).expect("blocklist");
    let excluded = BTreeSet::from([metafile.info_hash.as_str().to_owned()]);
    let assessment = AssessmentOptions {
        match_mode: MatchMode::Strict,
        fuzzy_size_threshold: 0.05,
        season_from_episodes: 0.75,
        include_single_episodes: true,
        info_hashes_to_exclude: &excluded,
        blocklist: &blocklist,
    };
    let injection = InjectionActionOptions {
        clients: &clients,
        output_dir: Some(&input_dir),
        link_dirs: &[],
        link_type: LinkType::Symlink,
        flat_linking: false,
        unwrap_symlinks: false,
        skip_recheck: true,
        match_mode: MatchMode::Strict,
        auto_resume_max_download: 0,
        ignore_non_relevant_files_to_resume: false,
        category: None,
        tags: vec![ClientLabel::new("cross-seed")],
        duplicate_categories: false,
    };

    let summary = inject_saved_torrents(
        &SavedInjectionOptions {
            input_dir: &input_dir,
            injection: &injection,
            assessment: &assessment,
            ignore_titles: false,
        },
        &[searchee],
        |_| Ok(()),
    )
    .expect("inject saved");

    assert_eq!(summary.scanned, 1);
    assert_eq!(summary.already_exists, 1);
    assert_eq!(summary.deleted, 1);
    assert_eq!(summary.failed, 0);
    assert!(!saved.path.exists());
    assert!(client.calls.lock().expect("calls").is_empty());
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn saved_torrent_injection_keeps_unrecognized_torrent_files() {
    let root = temp_path("saved-inject-arbitrary");
    let input_dir = root.join("saved");
    let data = root.join("data");
    fs::create_dir_all(&input_dir).expect("input");
    fs::create_dir_all(&data).expect("data");
    fs::write(data.join("Candidate.Release"), b"video-data").expect("source");
    let bytes = torrent_bytes("Candidate.Release", "https://tracker.example/announce", 10);
    let arbitrary = input_dir.join("manual-upload.torrent");
    fs::write(&arbitrary, &bytes).expect("arbitrary torrent");
    let mut searchee = Searchee::from_files(
        "Candidate.Release",
        "Candidate.Release",
        vec![File::new("Candidate.Release", 10)],
    );
    searchee.path = Some(Cow::Owned(data.display().to_string()));
    index_searchee_mtime(&mut searchee, &data.join("Candidate.Release"));
    let client = FakeClient::new("client");
    let clients: [&dyn TorrentClient; 1] = [&client];
    let blocklist = Blocklist::parse(&[]).expect("blocklist");
    let excluded = BTreeSet::new();
    let assessment = AssessmentOptions {
        match_mode: MatchMode::Strict,
        fuzzy_size_threshold: 0.05,
        season_from_episodes: 0.75,
        include_single_episodes: true,
        info_hashes_to_exclude: &excluded,
        blocklist: &blocklist,
    };
    let injection = InjectionActionOptions {
        clients: &clients,
        output_dir: Some(&input_dir),
        link_dirs: &[],
        link_type: LinkType::Symlink,
        flat_linking: false,
        unwrap_symlinks: false,
        skip_recheck: true,
        match_mode: MatchMode::Strict,
        auto_resume_max_download: 0,
        ignore_non_relevant_files_to_resume: false,
        category: None,
        tags: vec![ClientLabel::new("cross-seed")],
        duplicate_categories: false,
    };

    let summary = inject_saved_torrents(
        &SavedInjectionOptions {
            input_dir: &input_dir,
            injection: &injection,
            assessment: &assessment,
            ignore_titles: true,
        },
        &[searchee],
        |_| Ok(()),
    )
    .expect("inject saved");

    assert_eq!(summary.scanned, 1);
    assert_eq!(summary.injected, 0);
    assert_eq!(summary.deleted, 0);
    assert_eq!(summary.failed, 1);
    assert!(arbitrary.exists());
    assert!(client.calls.lock().expect("calls").is_empty());
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn saved_match_accepts_alternate_title_similarity() {
    let metadata = saved_metadata("Foreign Title");
    let metafile = saved_metafile("Foreign Title", vec![File::new("episode.mkv", 10)]);
    let searchee = Searchee::from_files(
        "Example Show (Foreign Title)",
        "Example Show (Foreign Title)",
        vec![File::new("episode.mkv", 10)],
    );
    let clients: [&dyn TorrentClient; 0] = [];
    let injection = test_injection_options(&clients);
    let blocklist = Blocklist::parse(&[]).expect("blocklist");
    let excluded = BTreeSet::new();
    let assessment = test_assessment_options(&blocklist, &excluded, MatchMode::Strict);
    let input_dir = PathBuf::from(".");
    let searchees = [searchee];

    let matched = best_saved_match(
        &metafile,
        &metadata,
        &searchees,
        &SavedInjectionOptions {
            input_dir: &input_dir,
            injection: &injection,
            assessment: &assessment,
            ignore_titles: false,
        },
    );

    assert!(matched.is_some());
}

#[test]
fn saved_match_filters_blocklisted_searchees() {
    let metadata = saved_metadata("Candidate Release");
    let metafile = saved_metafile("Candidate Release", vec![File::new("candidate.mkv", 10)]);
    let blocked = Searchee::from_files(
        "Blocked Candidate Release",
        "Candidate Release",
        vec![File::new("candidate.mkv", 10)],
    );
    let allowed = Searchee::from_files(
        "Candidate Release",
        "Candidate Release",
        vec![File::new("candidate.mkv", 10)],
    );
    let clients: [&dyn TorrentClient; 0] = [];
    let injection = test_injection_options(&clients);
    let blocklist = Blocklist::parse(&["name:blocked".to_owned()]).expect("blocklist");
    let excluded = BTreeSet::new();
    let assessment = test_assessment_options(&blocklist, &excluded, MatchMode::Strict);
    let input_dir = PathBuf::from(".");
    let searchees = [blocked, allowed];

    let (matched, decision) = best_saved_match(
        &metafile,
        &metadata,
        &searchees,
        &SavedInjectionOptions {
            input_dir: &input_dir,
            injection: &injection,
            assessment: &assessment,
            ignore_titles: false,
        },
    )
    .expect("saved match");

    assert_eq!(matched.name, "Candidate Release");
    assert_eq!(decision, Decision::Match);
}

#[test]
fn saved_match_sorts_by_source_and_file_count() {
    let metadata = saved_metadata("Candidate Release");
    let metafile = saved_metafile("Candidate Release", vec![File::new("candidate.mkv", 10)]);
    let mut data = Searchee::from_files(
        "Candidate Release",
        "Candidate Release",
        vec![
            File::new("candidate.mkv", 10),
            File::new("extra-feature.mkv", 5),
        ],
    );
    data.path = Some(Cow::Borrowed("/data/Candidate Release"));
    let mut torrent = Searchee::from_files(
        "Candidate Release",
        "Candidate Release",
        vec![File::new("candidate.mkv", 10)],
    );
    torrent.info_hash = Some(InfoHash::from_validated(
        "2222222222222222222222222222222222222222",
    ));
    let clients: [&dyn TorrentClient; 0] = [];
    let injection = test_injection_options(&clients);
    let blocklist = Blocklist::parse(&[]).expect("blocklist");
    let excluded = BTreeSet::new();
    let assessment = test_assessment_options(&blocklist, &excluded, MatchMode::Strict);
    let input_dir = PathBuf::from(".");
    let searchees = [data, torrent];

    let (matched, _) = best_saved_match(
        &metafile,
        &metadata,
        &searchees,
        &SavedInjectionOptions {
            input_dir: &input_dir,
            injection: &injection,
            assessment: &assessment,
            ignore_titles: false,
        },
    )
    .expect("saved match");

    assert!(matched.info_hash.is_some());

    let mut first = Searchee::from_files(
        "Candidate Release",
        "Candidate Release",
        vec![File::new("candidate.mkv", 10)],
    );
    first.path = Some(Cow::Borrowed("/data/one"));
    let mut more_files = Searchee::from_files(
        "Candidate Release",
        "Candidate Release",
        vec![
            File::new("candidate.mkv", 10),
            File::new("extra-feature.mkv", 5),
        ],
    );
    more_files.path = Some(Cow::Borrowed("/data/two"));
    let searchees = [first, more_files];

    let (matched, _) = best_saved_match(
        &metafile,
        &metadata,
        &searchees,
        &SavedInjectionOptions {
            input_dir: &input_dir,
            injection: &injection,
            assessment: &assessment,
            ignore_titles: false,
        },
    )
    .expect("saved match");

    assert_eq!(matched.files.len(), 2);
}

fn saved_metadata(name: &str) -> SavedTorrentMetadata<'static> {
    SavedTorrentMetadata::new(
        MediaType::Video,
        "Tracker",
        name.to_owned(),
        InfoHash::from_validated("1111111111111111111111111111111111111111"),
        false,
    )
}

fn saved_metafile(name: &str, files: Vec<File<'static>>) -> Metafile<'static> {
    Metafile::from_files(
        InfoHash::from_validated("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        name.to_owned(),
        name.to_owned(),
        1,
        files,
    )
}

fn test_assessment_options<'a>(
    blocklist: &'a Blocklist,
    excluded: &'a BTreeSet<String>,
    match_mode: MatchMode,
) -> AssessmentOptions<'a> {
    AssessmentOptions {
        match_mode,
        fuzzy_size_threshold: 0.05,
        season_from_episodes: 0.75,
        include_single_episodes: true,
        info_hashes_to_exclude: excluded,
        blocklist,
    }
}

fn test_injection_options<'a>(clients: &'a [&'a dyn TorrentClient]) -> InjectionActionOptions<'a> {
    InjectionActionOptions {
        clients,
        output_dir: None,
        link_dirs: &[],
        link_type: LinkType::Symlink,
        flat_linking: false,
        unwrap_symlinks: false,
        skip_recheck: true,
        match_mode: MatchMode::Strict,
        auto_resume_max_download: 0,
        ignore_non_relevant_files_to_resume: false,
        category: None,
        tags: Vec::new(),
        duplicate_categories: false,
    }
}

struct FakeClient {
    metadata: TorrentClientMetadata<'static>,
    existing: bool,
    download_dir: Result<PathBuf, ClientErrorCode>,
    all_download_dirs: BTreeMap<String, PathBuf>,
    all_torrents: Vec<ClientTorrent<'static>>,
    calls: Mutex<Vec<&'static str>>,
    last_options: Mutex<Option<InjectionOptions>>,
}

impl FakeClient {
    fn new(host: &str) -> Self {
        Self {
            metadata: TorrentClientMetadata::new(
                host.to_owned(),
                0,
                TorrentClientKind::QBittorrent,
                false,
                "fake",
            ),
            existing: false,
            download_dir: Ok(PathBuf::from("/downloads")),
            all_download_dirs: BTreeMap::new(),
            all_torrents: Vec::new(),
            calls: Mutex::new(Vec::new()),
            last_options: Mutex::new(None),
        }
    }

    fn with_priority(mut self, priority: u16) -> Self {
        self.metadata.priority = priority;
        self
    }

    fn with_download_dir(mut self, info_hash: &str, path: PathBuf) -> Self {
        self.all_download_dirs.insert(info_hash.to_owned(), path);
        self
    }

    fn with_torrent(mut self, torrent: ClientTorrent<'static>) -> Self {
        self.all_torrents.push(torrent);
        self
    }
}

impl TorrentClient for FakeClient {
    fn metadata(&self) -> &TorrentClientMetadata<'_> {
        &self.metadata
    }

    fn is_torrent_in_client(&self, _info_hash: &InfoHash<'_>) -> crate::Result<bool> {
        Ok(self.existing)
    }

    fn is_torrent_complete(&self, _info_hash: &InfoHash<'_>) -> crate::Result<bool> {
        Ok(true)
    }

    fn is_torrent_checking(&self, _info_hash: &InfoHash<'_>) -> crate::Result<bool> {
        Ok(false)
    }

    fn get_all_torrents(&self) -> crate::Result<Vec<ClientTorrent<'static>>> {
        Ok(self.all_torrents.clone())
    }

    fn get_download_dir(
        &self,
        _metafile: &Metafile<'_>,
        _options: DownloadDirOptions,
    ) -> crate::Result<Result<PathBuf, ClientErrorCode>> {
        Ok(self.download_dir.clone())
    }

    fn get_all_download_dirs(&self) -> crate::Result<BTreeMap<String, PathBuf>> {
        Ok(self.all_download_dirs.clone())
    }

    fn inject(
        &self,
        _new_torrent: &NewTorrent<'_>,
        _searchee: &Searchee<'_>,
        _decision: Decision,
        options: &InjectionOptions,
    ) -> crate::Result<InjectionResult> {
        self.calls
            .lock()
            .map_err(|_error| super::action_error("calls lock poisoned"))?
            .push("inject");
        *self
            .last_options
            .lock()
            .map_err(|_error| super::action_error("options lock poisoned"))? =
            Some(options.clone());
        Ok(InjectionResult::Injected)
    }

    fn recheck_torrent(&self, _info_hash: &InfoHash<'_>) -> crate::Result<()> {
        self.calls
            .lock()
            .map_err(|_error| super::action_error("calls lock poisoned"))?
            .push("recheck");
        Ok(())
    }

    fn resume_injection(
        &self,
        metafile: &Metafile<'_>,
        _decision: Decision,
        options: ResumeOptions,
    ) -> crate::Result<()> {
        let remaining = self
            .all_torrents
            .iter()
            .find(|torrent| torrent.info_hash == metafile.info_hash)
            .map(|torrent| if torrent.complete { 0 } else { metafile.length })
            .unwrap_or(0);
        if remaining > options.max_remaining_bytes {
            return Ok(());
        }
        self.calls
            .lock()
            .map_err(|_error| super::action_error("calls lock poisoned"))?
            .push("resume");
        Ok(())
    }

    fn validate_config(&self) -> crate::Result<()> {
        Ok(())
    }
}

fn torrent_bytes(name: &str, announce: &str, length: u64) -> Vec<u8> {
    format!(
            "d8:announce{}:{}4:infod6:lengthi{}e4:name{}:{}12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaaee",
            announce.len(),
            announce,
            length,
            name.len(),
            name
        )
        .into_bytes()
}

fn temp_path(name: &str) -> PathBuf {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("sporos-{name}-{millis}"))
}

fn index_searchee_mtime(searchee: &mut Searchee<'_>, path: &std::path::Path) {
    let metadata = fs::metadata(path).expect("source metadata");
    searchee.mtime_millis = super::metadata_mtime_millis(&metadata);
}

fn client_searchee(host: &'static str) -> Searchee<'static> {
    let mut searchee = Searchee::from_files(
        "Client.Source",
        "Client.Source",
        vec![File::new("file.mkv", 7)],
    );
    searchee.client = Some(ClientTorrentMetadata::new(
        host,
        "/downloads",
        None,
        Vec::new(),
        Vec::<Cow<'static, str>>::new(),
    ));
    searchee.info_hash = Some(InfoHash::from_validated(
        "0123456789abcdef0123456789abcdef01234567",
    ));
    searchee
}
