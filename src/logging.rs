#![cfg_attr(
    test,
    expect(
        clippy::unwrap_in_result,
        reason = "test writer mutex poisoning behavior is tracked for cleanup"
    )
)]

use std::io;

use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::util::SubscriberInitExt;

const DEFAULT_FILTER: &str = "warn,sporos=info";

pub fn init_from_env() -> Result<(), tracing_subscriber::util::TryInitError> {
    subscriber(default_filter(), io::stderr).try_init()
}

fn default_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|error| {
        eprintln!("sporos: ignoring invalid RUST_LOG: {error}");
        EnvFilter::new(DEFAULT_FILTER)
    })
}

fn subscriber<W>(filter: EnvFilter, writer: W) -> impl tracing::Subscriber + Send + Sync + 'static
where
    W: for<'writer> MakeWriter<'writer> + Send + Sync + 'static,
{
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(writer)
        .with_ansi(false)
        .with_target(true)
        .finish()
}

#[cfg(test)]
mod tests {
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};

    use super::*;

    #[test]
    fn subscriber_emits_runtime_events_to_configured_writer() {
        let writer = SharedWriter::default();
        let captured = writer.captured();
        let subscriber = subscriber(EnvFilter::new("warn,sporos=info"), writer);

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: "sporos::logging_test", answer = 42, "runtime log captured");
            tracing::debug!(target: "sporos::logging_test", "debug log suppressed");
            tracing::warn!(target: "external_dependency", "dependency warning captured");
        });

        let output = String::from_utf8(captured.lock().unwrap().clone()).unwrap();
        assert!(output.contains("runtime log captured"));
        assert!(output.contains("answer=42"));
        assert!(output.contains("dependency warning captured"));
        assert!(!output.contains("debug log suppressed"));
        assert!(!output.contains("\u{1b}["));
    }

    #[derive(Clone, Default)]
    struct SharedWriter {
        captured: Arc<Mutex<Vec<u8>>>,
    }

    impl SharedWriter {
        fn captured(&self) -> Arc<Mutex<Vec<u8>>> {
            Arc::clone(&self.captured)
        }
    }

    struct SharedWriteGuard {
        captured: Arc<Mutex<Vec<u8>>>,
    }

    impl Write for SharedWriteGuard {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.captured.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> MakeWriter<'writer> for SharedWriter {
        type Writer = SharedWriteGuard;

        fn make_writer(&'writer self) -> Self::Writer {
            SharedWriteGuard {
                captured: Arc::clone(&self.captured),
            }
        }
    }
}
