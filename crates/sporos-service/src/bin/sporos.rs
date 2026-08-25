use std::process::ExitCode;

use sporos_service::app::{display_error, init_logging, run};
use sporos_service::config::Config;

#[tokio::main]
async fn main() -> ExitCode {
    let config = match Config::load() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("configuration error: {}", display_error(&error));
            return ExitCode::FAILURE;
        }
    };
    if let Err(error) = init_logging(&config.logging) {
        eprintln!("logging error: {}", display_error(&error));
        return ExitCode::FAILURE;
    }
    match run(config, shutdown_signal()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(service = "sporos", error = %display_error(&error), "service stopped");
            ExitCode::FAILURE
        }
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = terminate.recv() => {}
            result = tokio::signal::ctrl_c() => {
                if let Err(error) = result {
                    tracing::error!(service = "sporos", error = %display_error(&error), "SIGINT listener failed");
                }
            }
        }
    }
    #[cfg(not(unix))]
    {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(service = "sporos", error = %display_error(&error), "shutdown signal listener failed");
        }
    }
}
