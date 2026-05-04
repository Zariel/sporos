//! Runtime-owned bounded queues and worker lifecycle management.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use tokio::{
    sync::{Mutex, mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

pub const JOB_QUEUE_CAPACITY: usize = 8;
pub const WEBHOOK_QUEUE_CAPACITY: usize = 64;
pub const REVERSE_LOOKUP_QUEUE_CAPACITY: usize = 256;
pub const INJECTION_QUEUE_CAPACITY: usize = 128;
pub const BLOCKING_LOCAL_QUEUE_CAPACITY: usize = 64;
pub const BLOCKING_FILESYSTEM_QUEUE_CAPACITY: usize = 64;
pub const BLOCKING_TORRENT_IO_QUEUE_CAPACITY: usize = 64;
pub const BLOCKING_LINKING_QUEUE_CAPACITY: usize = 32;
pub const BLOCKING_MATCHING_QUEUE_CAPACITY: usize = 32;

type RuntimeTaskFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
type RuntimeTaskFn = Box<dyn FnOnce(CancellationToken) -> RuntimeTaskFuture + Send + 'static>;

/// Error returned when a bounded runtime queue cannot accept work.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum QueueSubmitError {
    /// The queue is at capacity.
    Full {
        /// Queue name.
        queue: &'static str,
        /// Command kind.
        kind: &'static str,
    },
    /// The queue worker has shut down.
    Closed {
        /// Queue name.
        queue: &'static str,
        /// Command kind.
        kind: &'static str,
    },
}

impl std::fmt::Display for QueueSubmitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full { queue, kind } => {
                write!(formatter, "{queue} queue is full for {kind}")
            }
            Self::Closed { queue, kind } => {
                write!(formatter, "{queue} queue is closed for {kind}")
            }
        }
    }
}

impl std::error::Error for QueueSubmitError {}

/// Error returned by a runtime blocking executor submission.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum BlockingTaskError {
    /// The bounded executor queue did not accept the task.
    Queue(QueueSubmitError),
    /// The task was cancelled before its result was returned.
    Cancelled {
        /// Executor name.
        executor: &'static str,
        /// Task kind.
        kind: &'static str,
    },
    /// The blocking task panicked.
    Panicked {
        /// Executor name.
        executor: &'static str,
        /// Task kind.
        kind: &'static str,
    },
}

impl std::fmt::Display for BlockingTaskError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Queue(error) => write!(formatter, "{error}"),
            Self::Cancelled { executor, kind } => {
                write!(formatter, "{executor} blocking task cancelled for {kind}")
            }
            Self::Panicked { executor, kind } => {
                write!(formatter, "{executor} blocking task panicked for {kind}")
            }
        }
    }
}

impl std::error::Error for BlockingTaskError {}

struct RuntimeTask {
    kind: &'static str,
    run: RuntimeTaskFn,
}

#[derive(Debug, Default)]
struct RuntimeQueueMetrics {
    enqueued: AtomicUsize,
    rejected: AtomicUsize,
    started: AtomicUsize,
    finished: AtomicUsize,
    cancelled: AtomicUsize,
}

/// Snapshot of one runtime queue's observable counters.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub struct RuntimeQueueStats {
    /// Commands accepted by the queue.
    pub enqueued: usize,
    /// Commands rejected because the queue was full or closed.
    pub rejected: usize,
    /// Commands started by the worker.
    pub started: usize,
    /// Commands that completed normally.
    pub finished: usize,
    /// Commands cancelled after start.
    pub cancelled: usize,
}

/// Cloneable handle for one bounded runtime queue.
#[derive(Clone)]
pub struct RuntimeTaskQueue {
    name: &'static str,
    capacity: usize,
    sender: mpsc::Sender<RuntimeTask>,
    metrics: Arc<RuntimeQueueMetrics>,
}

impl RuntimeTaskQueue {
    /// Queue name used in tracing and errors.
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// Maximum number of queued commands.
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Return current queue counters.
    pub fn stats(&self) -> RuntimeQueueStats {
        RuntimeQueueStats {
            enqueued: self.metrics.enqueued.load(Ordering::Relaxed),
            rejected: self.metrics.rejected.load(Ordering::Relaxed),
            started: self.metrics.started.load(Ordering::Relaxed),
            finished: self.metrics.finished.load(Ordering::Relaxed),
            cancelled: self.metrics.cancelled.load(Ordering::Relaxed),
        }
    }

    /// Submit one async task without awaiting queue capacity.
    pub fn try_submit<F, Fut>(&self, kind: &'static str, task: F) -> Result<(), QueueSubmitError>
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let command = RuntimeTask {
            kind,
            run: Box::new(move |shutdown| Box::pin(task(shutdown))),
        };
        match self.sender.try_send(command) {
            Ok(()) => {
                self.metrics.enqueued.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(
                    queue = self.name,
                    kind,
                    capacity = self.capacity,
                    enqueue_result = "accepted",
                    "runtime command enqueued",
                );
                Ok(())
            }
            Err(error) => {
                self.metrics.rejected.fetch_add(1, Ordering::Relaxed);
                let error = match error {
                    mpsc::error::TrySendError::Full(_command) => QueueSubmitError::Full {
                        queue: self.name,
                        kind,
                    },
                    mpsc::error::TrySendError::Closed(_command) => QueueSubmitError::Closed {
                        queue: self.name,
                        kind,
                    },
                };
                tracing::warn!(
                    queue = self.name,
                    kind,
                    capacity = self.capacity,
                    enqueue_result = "rejected",
                    error = %error,
                    "runtime command rejected",
                );
                Err(error)
            }
        }
    }
}

/// Cloneable handle for one named bounded blocking executor.
#[derive(Clone)]
pub struct RuntimeBlockingExecutor {
    queue: RuntimeTaskQueue,
}

impl RuntimeBlockingExecutor {
    /// Executor name used in tracing and errors.
    pub const fn name(&self) -> &'static str {
        self.queue.name()
    }

    /// Maximum number of queued blocking tasks.
    pub const fn capacity(&self) -> usize {
        self.queue.capacity()
    }

    /// Return current executor queue counters.
    pub fn stats(&self) -> RuntimeQueueStats {
        self.queue.stats()
    }

    /// Submit one blocking task and await its returned value.
    pub async fn submit<T, F>(&self, kind: &'static str, task: F) -> Result<T, BlockingTaskError>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let executor = self.name();
        let (result_sender, result_receiver) = oneshot::channel();
        self.queue
            .try_submit(kind, move |_shutdown| async move {
                let result = tokio::task::spawn_blocking(task).await;
                if result_sender.send(result).is_err() {
                    tracing::debug!(executor, kind, "blocking task result receiver dropped");
                }
            })
            .map_err(BlockingTaskError::Queue)?;
        match result_receiver.await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(_error)) => Err(BlockingTaskError::Panicked { executor, kind }),
            Err(_error) => Err(BlockingTaskError::Cancelled { executor, kind }),
        }
    }
}

/// Named blocking executors for local filesystem and CPU-heavy work.
pub struct RuntimeBlockingExecutors {
    /// Filesystem traversal, metadata reads, and directory indexing.
    pub filesystem: RuntimeBlockingExecutor,
    /// Torrent metafile parsing and cache IO.
    pub torrent_io: RuntimeBlockingExecutor,
    /// Link creation, repair, cleanup, and related path checks.
    pub linking: RuntimeBlockingExecutor,
    /// CPU-heavy matching and fuzzy filtering.
    pub matching: RuntimeBlockingExecutor,
}

/// Queue handles exposed to daemon, API, and scheduler orchestration.
pub struct RuntimeQueues {
    /// Accepted scheduled job bodies.
    pub jobs: RuntimeTaskQueue,
    /// Validated webhook and API background work.
    pub webhooks: RuntimeTaskQueue,
    /// Shared RSS and announce reverse lookup work.
    pub reverse_lookup: RuntimeTaskQueue,
    /// Serialized torrent-client mutation work.
    pub injection: RuntimeTaskQueue,
    /// Blocking local filesystem and CPU work.
    pub blocking_local: RuntimeTaskQueue,
}

/// Daemon-owned runtime service container.
pub struct RuntimeServices {
    shutdown: CancellationToken,
    queues: RuntimeQueues,
    blocking: RuntimeBlockingExecutors,
    handles: Mutex<Vec<JoinHandle<()>>>,
}

impl RuntimeServices {
    /// Start runtime workers using a shutdown token owned by the daemon.
    pub fn start(shutdown: CancellationToken) -> Arc<Self> {
        let (jobs, jobs_worker) =
            RuntimeTaskQueue::new("jobs", JOB_QUEUE_CAPACITY, shutdown.child_token());
        let (webhooks, webhooks_worker) =
            RuntimeTaskQueue::new("webhooks", WEBHOOK_QUEUE_CAPACITY, shutdown.child_token());
        let (reverse_lookup, reverse_lookup_worker) = RuntimeTaskQueue::new(
            "reverse_lookup",
            REVERSE_LOOKUP_QUEUE_CAPACITY,
            shutdown.child_token(),
        );
        let (injection, injection_worker) = RuntimeTaskQueue::new(
            "injection",
            INJECTION_QUEUE_CAPACITY,
            shutdown.child_token(),
        );
        let (blocking_local, blocking_local_worker) = RuntimeTaskQueue::new(
            "blocking_local",
            BLOCKING_LOCAL_QUEUE_CAPACITY,
            shutdown.child_token(),
        );
        let (blocking_filesystem, blocking_filesystem_worker) = RuntimeTaskQueue::new(
            "blocking_filesystem",
            BLOCKING_FILESYSTEM_QUEUE_CAPACITY,
            shutdown.child_token(),
        );
        let (blocking_torrent_io, blocking_torrent_io_worker) = RuntimeTaskQueue::new(
            "blocking_torrent_io",
            BLOCKING_TORRENT_IO_QUEUE_CAPACITY,
            shutdown.child_token(),
        );
        let (blocking_linking, blocking_linking_worker) = RuntimeTaskQueue::new(
            "blocking_linking",
            BLOCKING_LINKING_QUEUE_CAPACITY,
            shutdown.child_token(),
        );
        let (blocking_matching, blocking_matching_worker) = RuntimeTaskQueue::new(
            "blocking_matching",
            BLOCKING_MATCHING_QUEUE_CAPACITY,
            shutdown.child_token(),
        );

        Arc::new(Self {
            shutdown,
            queues: RuntimeQueues {
                jobs,
                webhooks,
                reverse_lookup,
                injection,
                blocking_local,
            },
            blocking: RuntimeBlockingExecutors {
                filesystem: RuntimeBlockingExecutor {
                    queue: blocking_filesystem,
                },
                torrent_io: RuntimeBlockingExecutor {
                    queue: blocking_torrent_io,
                },
                linking: RuntimeBlockingExecutor {
                    queue: blocking_linking,
                },
                matching: RuntimeBlockingExecutor {
                    queue: blocking_matching,
                },
            },
            handles: Mutex::new(vec![
                jobs_worker,
                webhooks_worker,
                reverse_lookup_worker,
                injection_worker,
                blocking_local_worker,
                blocking_filesystem_worker,
                blocking_torrent_io_worker,
                blocking_linking_worker,
                blocking_matching_worker,
            ]),
        })
    }

    /// Borrow runtime queue handles.
    pub const fn queues(&self) -> &RuntimeQueues {
        &self.queues
    }

    /// Borrow named blocking executor handles.
    pub const fn blocking(&self) -> &RuntimeBlockingExecutors {
        &self.blocking
    }

    /// Return a child cancellation token for runtime-aware helper code.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.shutdown.child_token()
    }

    /// Cancel workers and wait for their tasks to finish.
    pub async fn shutdown(&self) {
        let started_at = Instant::now();
        tracing::info!("runtime worker shutdown starting");
        self.shutdown.cancel();
        let mut handles = self.handles.lock().await;
        let worker_count = handles.len();
        while let Some(handle) = handles.pop() {
            if let Err(error) = handle.await {
                tracing::error!("runtime worker task failed: {error}");
            }
        }
        tracing::info!(
            workers = worker_count,
            elapsed_ms = started_at.elapsed().as_millis(),
            "runtime worker shutdown complete"
        );
    }
}

impl RuntimeTaskQueue {
    fn new(
        name: &'static str,
        capacity: usize,
        shutdown: CancellationToken,
    ) -> (Self, JoinHandle<()>) {
        let (sender, receiver) = mpsc::channel(capacity);
        let metrics = Arc::new(RuntimeQueueMetrics::default());
        let queue = Self {
            name,
            capacity,
            sender,
            metrics: Arc::clone(&metrics),
        };
        let worker = tokio::spawn(run_worker(name, shutdown, receiver, metrics));
        (queue, worker)
    }
}

async fn run_worker(
    queue: &'static str,
    shutdown: CancellationToken,
    mut receiver: mpsc::Receiver<RuntimeTask>,
    metrics: Arc<RuntimeQueueMetrics>,
) {
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            command = receiver.recv() => {
                let Some(command) = command else {
                    break;
                };
                let kind = command.kind;
                let queued_at = Instant::now();
                metrics.started.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(queue, kind, "runtime command started");
                let command_shutdown = shutdown.child_token();
                let started_at = Instant::now();
                tokio::select! {
                    () = shutdown.cancelled() => {
                        command_shutdown.cancel();
                        metrics.cancelled.fetch_add(1, Ordering::Relaxed);
                        tracing::debug!(
                            queue,
                            kind,
                            queued_ms = queued_at.elapsed().as_millis(),
                            "runtime command cancelled before completion",
                        );
                    }
                    () = (command.run)(command_shutdown.clone()) => {
                        metrics.finished.fetch_add(1, Ordering::Relaxed);
                        tracing::debug!(
                            queue,
                            kind,
                            queued_ms = queued_at.elapsed().as_millis(),
                            elapsed_ms = started_at.elapsed().as_millis(),
                            "runtime command finished",
                        );
                    }
                }
            }
        }
    }
    tracing::debug!(queue, "runtime worker stopped");
}

#[cfg(test)]
mod tests {
    use super::{
        BlockingTaskError, QueueSubmitError, RuntimeBlockingExecutor, RuntimeQueueMetrics,
        RuntimeServices, RuntimeTaskQueue,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn runtime_queue_runs_submitted_work() {
        let services = RuntimeServices::start(CancellationToken::new());
        let ran = Arc::new(AtomicUsize::new(0));
        let ran_task = Arc::clone(&ran);

        services
            .queues()
            .webhooks
            .try_submit("test", move |_shutdown| async move {
                ran_task.fetch_add(1, Ordering::SeqCst);
            })
            .expect("submit");

        for _attempt in 0..10 {
            if ran.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(ran.load(Ordering::SeqCst), 1);
        services.shutdown().await;
    }

    #[tokio::test]
    async fn runtime_queue_reports_full_capacity() {
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        let metrics = Arc::new(RuntimeQueueMetrics::default());
        let queue = RuntimeTaskQueue {
            name: "webhooks",
            capacity: 1,
            sender,
            metrics,
        };

        queue
            .try_submit("held", |_shutdown| async {})
            .expect("submit held task");

        let error = queue
            .try_submit("overflow", |_shutdown| async {})
            .expect_err("queue is full");
        assert_eq!(
            error,
            QueueSubmitError::Full {
                queue: "webhooks",
                kind: "overflow",
            }
        );
        assert_eq!(queue.stats().enqueued, 1);
        assert_eq!(queue.stats().rejected, 1);
    }

    #[tokio::test]
    async fn runtime_queue_stats_track_worker_lifecycle() {
        let services = RuntimeServices::start(CancellationToken::new());
        services
            .queues()
            .webhooks
            .try_submit("observed", |_shutdown| async {})
            .expect("submit");

        for _attempt in 0..10 {
            let stats = services.queues().webhooks.stats();
            if stats.finished == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }

        let stats = services.queues().webhooks.stats();
        assert_eq!(stats.enqueued, 1);
        assert_eq!(stats.started, 1);
        assert_eq!(stats.finished, 1);
        assert_eq!(stats.cancelled, 0);
        services.shutdown().await;
    }

    #[tokio::test]
    async fn blocking_executor_returns_values_and_task_errors() {
        let services = RuntimeServices::start(CancellationToken::new());

        let value = services
            .blocking()
            .matching
            .submit("score", || 7usize)
            .await
            .expect("blocking result");
        assert_eq!(value, 7);

        let task_error = services
            .blocking()
            .matching
            .submit("fallible", || -> Result<(), &'static str> { Err("failed") })
            .await
            .expect("join succeeds");
        assert_eq!(task_error, Err("failed"));
        services.shutdown().await;
    }

    #[tokio::test]
    async fn blocking_executor_reports_panics() {
        let services = RuntimeServices::start(CancellationToken::new());

        let error = services
            .blocking()
            .linking
            .submit("panic", || panic!("blocking panic"))
            .await
            .expect_err("panic is reported");

        assert_eq!(
            error,
            BlockingTaskError::Panicked {
                executor: "blocking_linking",
                kind: "panic",
            }
        );
        services.shutdown().await;
    }

    #[tokio::test]
    async fn blocking_executor_reports_cancelled_queued_work() {
        let shutdown = CancellationToken::new();
        let (queue, worker) = RuntimeTaskQueue::new("blocking_test", 1, shutdown.child_token());
        let executor = RuntimeBlockingExecutor {
            queue: queue.clone(),
        };
        queue
            .try_submit("hold", |_shutdown| std::future::pending())
            .expect("hold worker");

        for _attempt in 0..10 {
            if queue.stats().started == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }

        let queued = tokio::spawn(async move { executor.submit("queued", || 1usize).await });
        for _attempt in 0..10 {
            if queue.stats().enqueued == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(queue.stats().enqueued, 2);

        shutdown.cancel();
        let error = queued
            .await
            .expect("queued task joins")
            .expect_err("queued task cancelled");
        assert_eq!(
            error,
            BlockingTaskError::Cancelled {
                executor: "blocking_test",
                kind: "queued",
            }
        );
        worker.await.expect("worker joins");
    }
}
