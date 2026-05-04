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
    fn initialize_push_notifier(&self, _config: &RuntimeConfig) -> crate::Result<()> {
        Ok(())
    }

    /// Validate configured torrent clients.
    fn validate_clients(&self, _config: &RuntimeConfig) -> crate::Result<()> {
        Ok(())
    }

    /// Validate configured indexer URLs and state.
    fn validate_indexers(&self, _config: &RuntimeConfig) -> crate::Result<()> {
        Ok(())
    }

    /// Validate configured Arr URLs and state.
    fn validate_arrs(&self, _config: &RuntimeConfig) -> crate::Result<()> {
        Ok(())
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
    fn initialize_push_notifier(&self, config: &RuntimeConfig) -> crate::Result<()> {
        let report = crate::notifications::NotificationSender::from_config_with_timeout(
            config,
            Redactor::from_config(config),
            std::time::Duration::from_secs(10),
        )?
        .validate_startup_report();
        if report.failed > 0 {
            tracing::warn!(
                dependency = "notification",
                failed = report.failed,
                attempted = report.attempted,
                "notification startup validation failed; treating as transient"
            );
        }
        Ok(())
    }

    fn validate_clients(&self, config: &RuntimeConfig) -> crate::Result<()> {
        let redactor = Redactor::from_config(config);
        let clients = crate::clients::build_torrent_clients_with_torrent_dir(
            &config.torrent_clients,
            config.search_timeout.map(std::time::Duration::from_millis),
            config.torrent_dir.as_deref(),
        )?;
        for client in clients {
            if let Err(error) = client.validate_config() {
                tracing::warn!(
                    dependency = "torrent_client",
                    target = client.metadata().host.as_ref(),
                    error = redactor.redact(&error.to_string()),
                    "torrent client startup validation failed; treating as transient"
                );
            }
        }
        Ok(())
    }

    fn validate_indexers(&self, config: &RuntimeConfig) -> crate::Result<()> {
        let redactor = Redactor::from_config(config);
        let mut working = 0usize;
        for entry in &config.torznab {
            let indexer = crate::integrations::validate_torznab_config(entry)?;
            let caps = match crate::integrations::fetch_torznab_caps(&indexer) {
                Ok(caps) => caps,
                Err(error) => {
                    tracing::warn!(
                        dependency = "indexer",
                        target = indexer.url.as_str(),
                        error = redactor.redact(&error.to_string()),
                        "indexer startup validation failed; treating as transient"
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
        Ok(())
    }

    fn validate_arrs(&self, config: &RuntimeConfig) -> crate::Result<()> {
        let redactor = Redactor::from_config(config);
        for entry in &config.sonarr {
            let arr = crate::integrations::validate_arr_config(
                entry,
                crate::integrations::ArrKind::Sonarr,
            )?;
            if let Err(error) = crate::integrations::validate_arr_instance(
                &arr,
                Some(std::time::Duration::from_secs(10)),
            ) {
                tracing::warn!(
                    dependency = "arr",
                    target = arr.url.as_str(),
                    error = redactor.redact(&error.to_string()),
                    "Arr startup validation failed; treating as transient"
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
                tracing::warn!(
                    dependency = "arr",
                    target = arr.url.as_str(),
                    error = redactor.redact(&error.to_string()),
                    "Arr startup validation failed; treating as transient"
                );
            }
        }
        Ok(())
    }
}

/// Start minimal runtime resources.
pub fn minimal_runtime(app_dir: PathBuf) -> RuntimeContext {
    RuntimeContext {
        app_dir,
        mode: StartupMode::Minimal,
        config: None,
        redactor: Redactor::default(),
        cleanup_hooks: Vec::new(),
    }
}

/// Start full runtime resources and run validation hooks.
pub fn full_runtime(
    app_dir: PathBuf,
    config: RuntimeConfig,
    hooks: &impl StartupHooks,
) -> crate::Result<RuntimeContext> {
    initialize_logger_with_config(
        &app_dir,
        config.verbose,
        config.log_format.as_deref(),
        config.log_level.as_deref(),
    )?;
    check_config_paths(&config)?;
    hooks.initialize_push_notifier(&config)?;
    hooks.validate_clients(&config)?;
    hooks.validate_indexers(&config)?;
    hooks.validate_arrs(&config)?;

    Ok(RuntimeContext {
        app_dir,
        mode: StartupMode::Full,
        redactor: Redactor::from_config(&config),
        config: Some(config),
        cleanup_hooks: Vec::new(),
    })
}

/// Initialize stderr tracing without requiring app-directory log files.
pub fn initialize_logger(_app_dir: &Path, verbose: bool) -> crate::Result<()> {
    initialize_logger_with_config(_app_dir, verbose, None, None)
}

fn initialize_logger_with_config(
    _app_dir: &Path,
    verbose: bool,
    configured_format: Option<&str>,
    configured_level: Option<&str>,
) -> crate::Result<()> {
    let level = log_level_from_config_or_env(configured_level, verbose);
    let format = log_format_from_config_or_env(configured_format);
    let _already_initialized = LOGGER.get_or_init(|| match format {
        LogFormat::Text => {
            let subscriber = tracing_subscriber::fmt()
                .with_max_level(level)
                .with_target(false)
                .with_writer(std::io::stderr)
                .finish();
            let _result = tracing::subscriber::set_global_default(subscriber);
        }
        LogFormat::Json => {
            let subscriber = tracing_subscriber::fmt()
                .json()
                .with_max_level(level)
                .with_current_span(true)
                .with_span_list(true)
                .with_writer(std::io::stderr)
                .finish();
            let _result = tracing::subscriber::set_global_default(subscriber);
        }
    });
    Ok(())
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum LogFormat {
    Text,
    Json,
}

fn log_format_from_config_or_env(configured: Option<&str>) -> LogFormat {
    configured
        .and_then(parse_log_format)
        .or_else(|| {
            std::env::var("SPOROS__LOG_FORMAT")
                .ok()
                .and_then(|value| parse_log_format(&value))
        })
        .or_else(|| {
            std::env::var("SPOROS_LOG_FORMAT")
                .ok()
                .and_then(|value| parse_log_format(&value))
        })
        .unwrap_or(LogFormat::Text)
}

fn log_level_from_config_or_env(configured: Option<&str>, verbose: bool) -> Level {
    configured
        .and_then(parse_log_level)
        .or_else(|| {
            std::env::var("SPOROS__LOG_LEVEL")
                .ok()
                .and_then(|value| parse_log_level(&value))
        })
        .or_else(|| {
            std::env::var("SPOROS_LOG_LEVEL")
                .ok()
                .and_then(|value| parse_log_level(&value))
        })
        .or_else(|| {
            std::env::var("RUST_LOG")
                .ok()
                .and_then(|value| parse_log_level(&value))
        })
        .unwrap_or(if verbose { Level::TRACE } else { Level::INFO })
}

fn parse_log_format(value: &str) -> Option<LogFormat> {
    match value.trim().to_ascii_lowercase().as_str() {
        "text" | "plain" | "pretty" => Some(LogFormat::Text),
        "json" => Some(LogFormat::Json),
        _ => None,
    }
}

fn parse_log_level(value: &str) -> Option<Level> {
    match value.trim().to_ascii_lowercase().as_str() {
        "trace" | "verbose" => Some(Level::TRACE),
        "debug" => Some(Level::DEBUG),
        "info" => Some(Level::INFO),
        "warn" | "warning" => Some(Level::WARN),
        "error" => Some(Level::ERROR),
        _ => None,
    }
}

/// Check configured filesystem paths and create writable output/link paths.
pub fn check_config_paths(config: &RuntimeConfig) -> crate::Result<()> {
    ensure_read_write_dir(&config.state_dir, "state_dir")?;
    if let Some(parent) = config
        .database_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        ensure_read_write_dir(parent, "database_path parent")?;
    }
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
        LogFormat, Redactor, RuntimeStartupHooks, StartupHooks, StartupMode, check_config_paths,
        full_runtime, initialize_logger, minimal_runtime, parse_log_format, parse_log_level,
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
    fn startup_uses_stderr_logging_and_configured_output_dirs() {
        let root = temp_path("startup");
        let config = test_config(root.clone());

        initialize_logger(&root, false).expect("logger");
        check_config_paths(&config).expect("paths");

        assert!(!root.join("logs").exists());
        assert!(root.join("cross-seeds").exists());
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn parses_logging_format_and_level_controls() {
        assert_eq!(parse_log_format("json"), Some(LogFormat::Json));
        assert_eq!(parse_log_format("text"), Some(LogFormat::Text));
        assert_eq!(parse_log_format("bogus"), None);
        assert_eq!(parse_log_level("trace"), Some(tracing::Level::TRACE));
        assert_eq!(parse_log_level("verbose"), Some(tracing::Level::TRACE));
        assert_eq!(parse_log_level("debug"), Some(tracing::Level::DEBUG));
        assert_eq!(parse_log_level("info"), Some(tracing::Level::INFO));
        assert_eq!(parse_log_level("warning"), Some(tracing::Level::WARN));
        assert_eq!(parse_log_level("error"), Some(tracing::Level::ERROR));
        assert_eq!(parse_log_level("sporos=debug"), None);
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
        fn initialize_push_notifier(&self, _config: &RuntimeConfig) -> crate::Result<()> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn validate_clients(&self, _config: &RuntimeConfig) -> crate::Result<()> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn validate_indexers(&self, _config: &RuntimeConfig) -> crate::Result<()> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn validate_arrs(&self, _config: &RuntimeConfig) -> crate::Result<()> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct FailingHooks;

    impl StartupHooks for FailingHooks {
        fn validate_clients(&self, _config: &RuntimeConfig) -> crate::Result<()> {
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
            state_dir: root.clone(),
            database_path: root.join("sporos.db"),
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
            log_format: None,
            log_level: None,
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
