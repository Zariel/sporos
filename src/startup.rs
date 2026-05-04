//! Startup validation, logger setup, runtime wiring, and graceful shutdown.

use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::OnceLock,
    time::{SystemTime, UNIX_EPOCH},
};

use tracing::Level;

use crate::{SporosError, config::RuntimeConfig};

static LOGGER: OnceLock<()> = OnceLock::new();

/// Startup mode matching the documented runtime wrappers.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum StartupMode {
    /// DB migration plus command callback.
    Minimal,
    /// Minimal runtime plus logger, config, clients, integrations, and paths.
    Full,
}

/// External dependency family reported when startup enters degraded mode.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum StartupDependencyKind {
    /// Notification webhook dependency.
    Notification,
    /// Configured torrent client dependency.
    TorrentClient,
    /// Configured Torznab indexer dependency.
    Indexer,
    /// Configured Sonarr/Radarr dependency.
    Arr,
}

impl StartupDependencyKind {
    /// Stable lowercase label for status and metrics output.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Notification => "notification",
            Self::TorrentClient => "torrent_client",
            Self::Indexer => "indexer",
            Self::Arr => "arr",
        }
    }
}

/// One external dependency that failed startup validation but can recover later.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StartupDependencyDegradation {
    /// Dependency family.
    pub kind: StartupDependencyKind,
    /// Safe dependency identifier.
    pub target: String,
    /// Redacted reason captured during startup validation.
    pub reason: String,
}

/// Startup dependency policy state.
///
/// Invalid config, app directories, schema setup, and unsafe local paths still
/// fail fast. External dependency validation failures are captured here so the
/// daemon can start degraded and expose `ready == false` until normal runtime
/// paths observe recovery.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct StartupDependencyState {
    degraded: Vec<StartupDependencyDegradation>,
}

impl StartupDependencyState {
    /// Record one degraded dependency.
    pub fn push(
        &mut self,
        kind: StartupDependencyKind,
        target: impl Into<String>,
        reason: impl Into<String>,
    ) {
        self.degraded.push(StartupDependencyDegradation {
            kind,
            target: target.into(),
            reason: reason.into(),
        });
    }

    /// Merge another dependency state into this state.
    pub fn extend(&mut self, mut other: StartupDependencyState) {
        self.degraded.append(&mut other.degraded);
    }

    /// Whether startup saw transient dependency failures.
    pub fn is_degraded(&self) -> bool {
        !self.degraded.is_empty()
    }

    /// Kubernetes readiness policy from startup state alone.
    pub fn ready(&self) -> bool {
        !self.is_degraded()
    }

    /// Recorded degraded dependencies.
    pub fn degraded_dependencies(&self) -> &[StartupDependencyDegradation] {
        &self.degraded
    }
}

/// Redacts secrets from startup and integration diagnostics.
#[derive(Debug, Default, Clone)]
pub struct Redactor {
    secrets: Vec<String>,
}

impl Redactor {
    /// Build a redactor from configured secret-bearing values.
    pub fn from_config(config: &RuntimeConfig) -> Self {
        let mut secrets = Vec::new();
        if let Some(api_key) = &config.api_key {
            push_secret(&mut secrets, api_key);
        }
        for url in &config.notification_webhook_urls {
            push_url_secrets(&mut secrets, url);
        }
        for integration in config
            .torznab
            .iter()
            .chain(config.sonarr.iter())
            .chain(config.radarr.iter())
        {
            push_url_secrets(&mut secrets, &integration.url);
            push_secret(&mut secrets, &integration.api_key);
        }
        for client in &config.torrent_clients {
            push_url_secrets(&mut secrets, &client.url);
        }
        Self { secrets }
    }

    /// Redact configured secrets and common URL password positions.
    pub fn redact(&self, input: &str) -> String {
        let mut redacted = redact_url_passwords(input);
        for secret in &self.secrets {
            redacted = redacted.replace(secret, "[REDACTED]");
        }
        redacted
    }
}

/// Runtime resources initialized for a command.
pub struct RuntimeContext {
    /// App directory.
    pub app_dir: PathBuf,
    /// Runtime mode.
    pub mode: StartupMode,
    /// Normalized config for full runtime commands.
    pub config: Option<RuntimeConfig>,
    /// Secret redactor.
    pub redactor: Redactor,
    /// External dependency state captured during startup.
    pub dependency_state: StartupDependencyState,
    cleanup_hooks: Vec<Box<dyn FnOnce() + Send + 'static>>,
}

impl RuntimeContext {
    /// Register a cleanup hook to run during shutdown.
    pub fn push_cleanup(&mut self, hook: impl FnOnce() + Send + 'static) {
        self.cleanup_hooks.push(Box::new(hook));
    }

    /// Run cleanup hooks in reverse registration order.
    pub fn shutdown(mut self) {
        while let Some(hook) = self.cleanup_hooks.pop() {
            hook();
        }
    }
}

/// Hooks supplied by later runtime layers.
pub trait StartupHooks {
    /// Initialize push notification state.
    fn initialize_push_notifier(
        &self,
        _config: &RuntimeConfig,
    ) -> crate::Result<StartupDependencyState> {
        Ok(StartupDependencyState::default())
    }

    /// Validate configured torrent clients.
    fn validate_clients(&self, _config: &RuntimeConfig) -> crate::Result<StartupDependencyState> {
        Ok(StartupDependencyState::default())
    }

    /// Validate configured indexer URLs and state.
    fn validate_indexers(&self, _config: &RuntimeConfig) -> crate::Result<StartupDependencyState> {
        Ok(StartupDependencyState::default())
    }

    /// Validate configured Arr URLs and state.
    fn validate_arrs(&self, _config: &RuntimeConfig) -> crate::Result<StartupDependencyState> {
        Ok(StartupDependencyState::default())
    }
}

/// No-op hooks used before integrations are implemented.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopStartupHooks;

impl StartupHooks for NoopStartupHooks {}

/// Default hooks for full-runtime startup validation.
#[derive(Debug, Default, Clone, Copy)]
pub struct RuntimeStartupHooks;

impl StartupHooks for RuntimeStartupHooks {
    fn initialize_push_notifier(
        &self,
        config: &RuntimeConfig,
    ) -> crate::Result<StartupDependencyState> {
        let redactor = Redactor::from_config(config);
        let report = crate::notifications::NotificationSender::from_config_with_timeout(
            config,
            redactor.clone(),
            std::time::Duration::from_secs(10),
        )?
        .validate_startup_report();
        let mut state = StartupDependencyState::default();
        if report.failed > 0 {
            state.push(
                StartupDependencyKind::Notification,
                "configured_webhooks",
                redactor.redact(&format!(
                    "failed to validate {}/{} notification webhooks",
                    report.failed, report.attempted
                )),
            );
        }
        Ok(state)
    }

    fn validate_clients(&self, config: &RuntimeConfig) -> crate::Result<StartupDependencyState> {
        let redactor = Redactor::from_config(config);
        let clients = crate::clients::build_torrent_clients_with_torrent_dir(
            &config.torrent_clients,
            config.search_timeout.map(std::time::Duration::from_millis),
            config.torrent_dir.as_deref(),
        )?;
        let mut state = StartupDependencyState::default();
        for client in clients {
            if let Err(error) = client.validate_config() {
                state.push(
                    StartupDependencyKind::TorrentClient,
                    client.metadata().host.as_ref(),
                    redactor.redact(&error.to_string()),
                );
            }
        }
        Ok(state)
    }

    fn validate_indexers(&self, config: &RuntimeConfig) -> crate::Result<StartupDependencyState> {
        let redactor = Redactor::from_config(config);
        let mut state = StartupDependencyState::default();
        let mut working = 0usize;
        for entry in &config.torznab {
            let indexer = crate::integrations::validate_torznab_config(entry)?;
            let caps = match crate::integrations::fetch_torznab_caps(&indexer) {
                Ok(caps) => caps,
                Err(error) => {
                    state.push(
                        StartupDependencyKind::Indexer,
                        indexer.url.clone(),
                        redactor.redact(&error.to_string()),
                    );
                    continue;
                }
            };
            if caps.search
                || caps.tv_search
                || caps.movie_search
                || caps.music_search
                || caps.audio_search
                || caps.book_search
            {
                working += 1;
            } else {
                tracing::warn!(
                    indexer = indexer.url.as_str(),
                    "Torznab indexer has no searchable capabilities"
                );
            }
        }
        if !config.torznab.is_empty() && working == 0 {
            tracing::warn!("no configured Torznab indexers advertise search support");
        }
        Ok(state)
    }

    fn validate_arrs(&self, config: &RuntimeConfig) -> crate::Result<StartupDependencyState> {
        let redactor = Redactor::from_config(config);
        let mut state = StartupDependencyState::default();
        for entry in &config.sonarr {
            let arr = crate::integrations::validate_arr_config(
                entry,
                crate::integrations::ArrKind::Sonarr,
            )?;
            if let Err(error) = crate::integrations::validate_arr_instance(
                &arr,
                Some(std::time::Duration::from_secs(10)),
            ) {
                state.push(
                    StartupDependencyKind::Arr,
                    arr.url.clone(),
                    redactor.redact(&error.to_string()),
                );
            }
        }
        for entry in &config.radarr {
            let arr = crate::integrations::validate_arr_config(
                entry,
                crate::integrations::ArrKind::Radarr,
            )?;
            if let Err(error) = crate::integrations::validate_arr_instance(
                &arr,
                Some(std::time::Duration::from_secs(10)),
            ) {
                state.push(
                    StartupDependencyKind::Arr,
                    arr.url.clone(),
                    redactor.redact(&error.to_string()),
                );
            }
        }
        Ok(state)
    }
}

/// Start minimal runtime resources.
pub fn minimal_runtime(app_dir: PathBuf) -> RuntimeContext {
    RuntimeContext {
        app_dir,
        mode: StartupMode::Minimal,
        config: None,
        redactor: Redactor::default(),
        dependency_state: StartupDependencyState::default(),
        cleanup_hooks: Vec::new(),
    }
}

/// Start full runtime resources and run validation hooks.
pub fn full_runtime(
    app_dir: PathBuf,
    config: RuntimeConfig,
    hooks: &impl StartupHooks,
) -> crate::Result<RuntimeContext> {
    initialize_logger(&app_dir, config.verbose)?;
    check_config_paths(&config)?;
    let mut dependency_state = StartupDependencyState::default();
    dependency_state.extend(hooks.initialize_push_notifier(&config)?);
    dependency_state.extend(hooks.validate_clients(&config)?);
    dependency_state.extend(hooks.validate_indexers(&config)?);
    dependency_state.extend(hooks.validate_arrs(&config)?);
    for dependency in dependency_state.degraded_dependencies() {
        tracing::warn!(
            dependency = dependency.kind.as_str(),
            target = dependency.target.as_str(),
            reason = dependency.reason.as_str(),
            "startup dependency degraded"
        );
    }

    Ok(RuntimeContext {
        app_dir,
        mode: StartupMode::Full,
        redactor: Redactor::from_config(&config),
        dependency_state,
        config: Some(config),
        cleanup_hooks: Vec::new(),
    })
}

/// Initialize console tracing and create log files under `appDir()/logs`.
pub fn initialize_logger(app_dir: &Path, verbose: bool) -> crate::Result<()> {
    let logs_dir = app_dir.join("logs");
    fs::create_dir_all(&logs_dir)
        .map_err(|error| startup_error(format!("failed to create logs directory: {error}")))?;
    for file_name in ["error.log", "info.log", "verbose.log"] {
        let path = logs_dir.join(file_name);
        if !path.exists() {
            fs::File::create(&path)
                .map_err(|error| startup_error(format!("failed to create log file: {error}")))?;
        }
    }

    let level = if verbose { Level::TRACE } else { Level::INFO };
    let _already_initialized = LOGGER.get_or_init(|| {
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(level)
            .with_target(false)
            .finish();
        let _result = tracing::subscriber::set_global_default(subscriber);
    });
    Ok(())
}

/// Check configured filesystem paths and create writable output/link paths.
pub fn check_config_paths(config: &RuntimeConfig) -> crate::Result<()> {
    if let Some(torrent_dir) = &config.torrent_dir {
        verify_readable_dir(torrent_dir, "torrent_dir")?;
    }
    ensure_read_write_dir(&config.output_dir, "output_dir")?;
    for link_dir in &config.link_dirs {
        ensure_read_write_dir(link_dir, "link_dir")?;
    }
    for data_dir in &config.data_dirs {
        verify_readable_dir(data_dir, "data_dir")?;
    }
    if let Some(inject_dir) = &config.inject_dir {
        ensure_read_write_dir(inject_dir, "inject_dir")?;
    }
    for data_dir in &config.data_dirs {
        for link_dir in &config.link_dirs {
            verify_link_probe(data_dir, link_dir)?;
        }
    }
    Ok(())
}

fn verify_readable_dir(path: &Path, label: &str) -> crate::Result<()> {
    let metadata = fs::metadata(path)
        .map_err(|error| startup_error(format!("{label} is not readable: {error}")))?;
    if metadata.is_dir() {
        Ok(())
    } else {
        Err(startup_error(format!(
            "{label} is not a directory: {}",
            path.display()
        )))
    }
}

fn ensure_read_write_dir(path: &Path, label: &str) -> crate::Result<()> {
    fs::create_dir_all(path)
        .map_err(|error| startup_error(format!("failed to create {label}: {error}")))?;
    verify_readable_dir(path, label)?;
    let probe = create_unique_probe(path, ".sporos-write-test")
        .map_err(|error| startup_error(format!("{label} is not writable: {error}")))?;
    fs::remove_file(&probe)
        .map_err(|error| startup_error(format!("failed to remove {label} probe: {error}")))?;
    Ok(())
}

fn create_unique_probe(dir: &Path, prefix: &str) -> std::io::Result<PathBuf> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    for attempt in 0..128 {
        let path = dir.join(format!("{prefix}-{}-{nanos}-{attempt}", std::process::id()));
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(mut file) => {
                file.write_all(b"test")?;
                return Ok(path);
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "failed to allocate unique probe path",
    ))
}

fn verify_link_probe(data_dir: &Path, link_dir: &Path) -> crate::Result<()> {
    verify_readable_dir(data_dir, "data_dir")?;
    ensure_read_write_dir(link_dir, "link_dir")
}

fn push_secret(secrets: &mut Vec<String>, secret: &str) {
    if secret.len() >= 8 && !secrets.iter().any(|existing| existing == secret) {
        secrets.push(secret.to_owned());
    }
}

fn push_url_secrets(secrets: &mut Vec<String>, url: &str) {
    if let Some(credentials_end) = url.find('@') {
        if let Some(scheme_end) = url.find("://") {
            let credentials_start = scheme_end + 3;
            if credentials_start < credentials_end {
                if let Some(credentials) = url.get(credentials_start..credentials_end) {
                    if let Some((_, password)) = credentials.split_once(':') {
                        push_secret(secrets, password);
                    }
                }
            }
        }
    }
    for key in ["apikey=", "api_key=", "passkey=", "token="] {
        if let Some(start) = url.find(key) {
            let value_start = start + key.len();
            let Some(value_tail) = url.get(value_start..) else {
                continue;
            };
            let value_end = value_tail
                .find(['&', '#'])
                .map_or(url.len(), |offset| value_start + offset);
            if let Some(secret) = url.get(value_start..value_end) {
                push_secret(secrets, secret);
            }
        }
    }
}

fn redact_url_passwords(input: &str) -> String {
    let mut output = input.to_owned();
    let mut search_from = 0;
    while let Some(relative_at) = output.get(search_from..).and_then(|tail| tail.find('@')) {
        let at = search_from + relative_at;
        let Some(scheme) = output.get(..at).and_then(|head| head.rfind("://")) else {
            break;
        };
        let credentials_start = scheme + 3;
        let Some(colon) = output
            .get(credentials_start..at)
            .and_then(|credentials| credentials.find(':'))
        else {
            break;
        };
        let password_start = credentials_start + colon + 1;
        output.replace_range(password_start..at, "[REDACTED]");
        search_from = password_start + "[REDACTED]".len() + 1;
    }
    output
}

fn startup_error(message: impl Into<std::borrow::Cow<'static, str>>) -> SporosError {
    SporosError::Startup {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Redactor, RuntimeStartupHooks, StartupDependencyKind, StartupDependencyState, StartupHooks,
        StartupMode, check_config_paths, full_runtime, initialize_logger, minimal_runtime,
    };
    use crate::config::{Action, ApiIntegrationConfig, RuntimeConfig};
    use std::{
        fs,
        io::{BufRead, BufReader, Read, Write},
        net::{TcpListener, TcpStream},
        path::{Path, PathBuf},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        thread,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn redacts_api_keys_and_url_passwords() {
        let mut config = test_config(temp_path("redact"));
        config.api_key = Some("123456789012345678901234".to_owned());
        config.notification_webhook_urls =
            vec!["https://user:secret@example.test/hook?token=notificationtoken".to_owned()];
        config.torznab = vec![ApiIntegrationConfig {
            url: "https://indexer.example/api".to_owned(),
            api_key: "indexersecret".to_owned(),
        }];

        let redactor = Redactor::from_config(&config);
        let redacted = redactor.redact(
            "api=123456789012345678901234 url=https://user:secret@example.test/hook?token=notificationtoken indexer=indexersecret",
        );

        assert!(!redacted.contains("123456789012345678901234"));
        assert!(!redacted.contains("secret@"));
        assert!(!redacted.contains("notificationtoken"));
        assert!(!redacted.contains("indexersecret"));
    }

    #[test]
    fn startup_creates_logs_and_configured_output_dirs() {
        let root = temp_path("startup");
        let config = test_config(root.clone());

        initialize_logger(&root, false).expect("logger");
        check_config_paths(&config).expect("paths");

        assert!(root.join("logs/error.log").exists());
        assert!(root.join("cross-seeds").exists());
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn path_write_probe_does_not_clobber_existing_probe_name() {
        let root = temp_path("startup-probe-collision");
        let config = test_config(root.clone());
        fs::create_dir_all(&config.output_dir).expect("output dir");
        let existing_probe = config.output_dir.join(".sporos-write-test");
        fs::write(&existing_probe, b"user data").expect("existing probe");

        check_config_paths(&config).expect("paths");

        assert_eq!(
            fs::read(existing_probe).expect("existing probe"),
            b"user data"
        );
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn full_runtime_runs_hooks_and_shutdown_callbacks() {
        let root = temp_path("runtime");
        let config = test_config(root.clone());
        let hooks = CountingHooks::default();

        let mut runtime = full_runtime(root.clone(), config, &hooks).expect("runtime");
        assert_eq!(runtime.mode, StartupMode::Full);
        assert_eq!(hooks.count.load(Ordering::SeqCst), 4);
        assert!(runtime.dependency_state.ready());

        let cleanup_count = Arc::new(AtomicUsize::new(0));
        let cleanup_count_clone = Arc::clone(&cleanup_count);
        runtime.push_cleanup(move || {
            cleanup_count_clone.fetch_add(1, Ordering::SeqCst);
        });
        runtime.shutdown();

        assert_eq!(cleanup_count.load(Ordering::SeqCst), 1);
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn full_runtime_captures_transient_dependency_degradation() {
        let root = temp_path("runtime-degraded");
        let config = test_config(root.clone());
        let hooks = DegradedHooks;

        let runtime = full_runtime(root.clone(), config, &hooks).expect("runtime");

        assert!(!runtime.dependency_state.ready());
        assert_eq!(
            runtime.dependency_state.degraded_dependencies(),
            &[super::StartupDependencyDegradation {
                kind: StartupDependencyKind::Indexer,
                target: "https://indexer.example/api".to_owned(),
                reason: "connection refused".to_owned(),
            }]
        );
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn full_runtime_still_fails_fast_on_startup_errors() {
        let root = temp_path("runtime-fail-fast");
        let config = test_config(root.clone());
        let hooks = FailingHooks;

        let error = full_runtime(root.clone(), config, &hooks)
            .err()
            .expect("startup should fail");

        assert!(error.to_string().contains("invalid local startup state"));
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn runtime_hooks_validate_external_integrations() {
        let torznab = http_server(vec![http_response(
            "200 OK",
            &[("Content-Type", "application/xml")],
            r#"<caps><searching searchAvailable="yes" /><limits default="25" max="100" /></caps>"#,
        )]);
        let arr = http_server(vec![http_response(
            "200 OK",
            &[("Content-Type", "application/json")],
            r#"{"current":"4.0.0"}"#,
        )]);
        let notification = http_server(vec![http_response("204 No Content", &[], "")]);
        let mut config = test_config(temp_path("runtime-hooks"));
        config.torznab = vec![ApiIntegrationConfig {
            url: format!("{}/api", torznab.url),
            api_key: "indexer-secret".to_owned(),
        }];
        config.sonarr = vec![ApiIntegrationConfig {
            url: format!("{}/sonarr", arr.url),
            api_key: "arr-secret".to_owned(),
        }];
        config.notification_webhook_urls = vec![notification.url.clone()];
        let hooks = RuntimeStartupHooks;

        hooks
            .initialize_push_notifier(&config)
            .expect("notification validation");
        hooks
            .validate_indexers(&config)
            .expect("indexer validation");
        hooks.validate_arrs(&config).expect("arr validation");

        let notification_requests = notification.join();
        let torznab_requests = torznab.join();
        let arr_requests = arr.join();
        assert!(notification_requests[0].contains(r#""event":"STARTUP_VALIDATION""#));
        assert!(torznab_requests[0].contains("get /api?apikey=indexer-secret&t=caps "));
        assert!(arr_requests[0].contains("get /sonarr/api "));
        assert!(arr_requests[0].contains("x-api-key: arr-secret"));
    }

    #[test]
    fn minimal_runtime_has_no_config() {
        let runtime = minimal_runtime(PathBuf::from("/tmp/sporos-minimal"));

        assert_eq!(runtime.mode, StartupMode::Minimal);
        assert!(runtime.config.is_none());
    }

    #[derive(Default)]
    struct CountingHooks {
        count: AtomicUsize,
    }

    impl StartupHooks for CountingHooks {
        fn initialize_push_notifier(
            &self,
            _config: &RuntimeConfig,
        ) -> crate::Result<StartupDependencyState> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(StartupDependencyState::default())
        }

        fn validate_clients(
            &self,
            _config: &RuntimeConfig,
        ) -> crate::Result<StartupDependencyState> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(StartupDependencyState::default())
        }

        fn validate_indexers(
            &self,
            _config: &RuntimeConfig,
        ) -> crate::Result<StartupDependencyState> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(StartupDependencyState::default())
        }

        fn validate_arrs(&self, _config: &RuntimeConfig) -> crate::Result<StartupDependencyState> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(StartupDependencyState::default())
        }
    }

    struct DegradedHooks;

    impl StartupHooks for DegradedHooks {
        fn validate_indexers(
            &self,
            _config: &RuntimeConfig,
        ) -> crate::Result<StartupDependencyState> {
            let mut state = StartupDependencyState::default();
            state.push(
                StartupDependencyKind::Indexer,
                "https://indexer.example/api",
                "connection refused",
            );
            Ok(state)
        }
    }

    struct FailingHooks;

    impl StartupHooks for FailingHooks {
        fn validate_clients(
            &self,
            _config: &RuntimeConfig,
        ) -> crate::Result<StartupDependencyState> {
            Err(crate::SporosError::Startup {
                message: "invalid local startup state".into(),
            })
        }
    }

    struct TestHttpServer {
        url: String,
        handle: thread::JoinHandle<Vec<String>>,
    }

    impl TestHttpServer {
        fn join(self) -> Vec<String> {
            self.handle.join().expect("server")
        }
    }

    fn http_server(responses: Vec<String>) -> TestHttpServer {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("addr"));
        let handle = thread::spawn(move || {
            let mut requests = Vec::new();
            for response in responses {
                let (mut stream, _remote) = listener.accept().expect("accept");
                requests.push(read_http_request(&mut stream));
                stream.write_all(response.as_bytes()).expect("response");
            }
            requests
        });
        TestHttpServer { url, handle }
    }

    fn http_response(status: &str, headers: &[(&str, &str)], body: &str) -> String {
        let mut response = format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\n", body.len());
        for (name, value) in headers {
            response.push_str(name);
            response.push_str(": ");
            response.push_str(value);
            response.push_str("\r\n");
        }
        response.push_str("\r\n");
        response.push_str(body);
        response
    }

    fn read_http_request(stream: &mut TcpStream) -> String {
        let mut reader = BufReader::new(stream);
        let mut request = String::new();
        let mut content_length = 0usize;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).expect("line");
            if line.eq_ignore_ascii_case("\r\n") || line == "\n" {
                break;
            }
            if let Some((name, value)) = line.split_once(':') {
                if name.eq_ignore_ascii_case("content-length") {
                    content_length = value.trim().parse().expect("length");
                }
            }
            request.push_str(&line.to_ascii_lowercase());
        }
        let mut body = vec![0_u8; content_length];
        reader.read_exact(&mut body).expect("body");
        request.push_str(&String::from_utf8_lossy(&body));
        request
    }

    fn test_config(root: PathBuf) -> RuntimeConfig {
        RuntimeConfig {
            delay: 30,
            torznab: Vec::new(),
            use_client_torrents: false,
            data_dirs: Vec::new(),
            match_mode: crate::config::MatchMode::Strict,
            skip_recheck: true,
            auto_resume_max_download: 0,
            ignore_non_relevant_files_to_resume: false,
            link_category: None,
            link_dirs: Vec::new(),
            link_type: crate::config::LinkType::Symlink,
            flat_linking: false,
            max_data_depth: 2,
            torrent_dir: None,
            output_dir: root.join("cross-seeds"),
            inject_dir: None,
            ignore_titles: None,
            include_single_episodes: false,
            include_non_videos: false,
            fuzzy_size_threshold: 0.05,
            season_from_episodes: None,
            exclude_older: None,
            exclude_recent_search: None,
            action: Action::Save,
            torrent_clients: Vec::new(),
            duplicate_categories: false,
            notification_webhook_urls: Vec::new(),
            port: Some(2468),
            host: None,
            rss_cadence: None,
            search_cadence: None,
            snatch_timeout: None,
            snatch_retries: 2,
            search_timeout: None,
            search_limit: None,
            verbose: false,
            torrents: None,
            block_list: Vec::new(),
            api_key: None,
            sonarr: Vec::new(),
            radarr: Vec::new(),
        }
    }

    fn temp_path(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("sporos-{label}-{nanos}"))
    }

    fn _assert_path_send_sync(_: &Path) {}
}
