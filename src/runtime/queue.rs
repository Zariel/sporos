use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum QueueKind {
    Announcement,
    Search,
    Injection,
    Notification,
    Indexing,
}

impl QueueKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Announcement => "announcement",
            Self::Search => "search",
            Self::Injection => "injection",
            Self::Notification => "notification",
            Self::Indexing => "indexing",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct RuntimeQueueConfig {
    pub announcement_limit: NonZeroUsize,
    pub search_limit: NonZeroUsize,
    pub injection_limit: NonZeroUsize,
    pub notification_limit: NonZeroUsize,
    pub indexing_limit: NonZeroUsize,
}

impl RuntimeQueueConfig {
    pub fn limit_for(self, kind: QueueKind) -> NonZeroUsize {
        match kind {
            QueueKind::Announcement => self.announcement_limit,
            QueueKind::Search => self.search_limit,
            QueueKind::Injection => self.injection_limit,
            QueueKind::Notification => self.notification_limit,
            QueueKind::Indexing => self.indexing_limit,
        }
    }
}

impl Default for RuntimeQueueConfig {
    fn default() -> Self {
        Self {
            announcement_limit: nonzero(1_000),
            search_limit: nonzero(100),
            injection_limit: nonzero(100),
            notification_limit: nonzero(500),
            indexing_limit: nonzero(50),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct WorkerPoolConfig {
    pub queue: QueueKind,
    pub concurrency: NonZeroUsize,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct QueueStats {
    pub kind: QueueKind,
    pub capacity: usize,
    pub depth: usize,
    pub accepted: u64,
    pub rejected: u64,
    pub completed: u64,
    pub cancelled: u64,
}

#[derive(Debug)]
pub struct BoundedWorkQueue<T> {
    kind: QueueKind,
    capacity: usize,
    sender: mpsc::Sender<T>,
    depth: Arc<AtomicUsize>,
    metrics: Arc<QueueMetrics>,
}

#[derive(Debug)]
pub struct WorkReceiver<T> {
    kind: QueueKind,
    capacity: usize,
    receiver: mpsc::Receiver<T>,
    depth: Arc<AtomicUsize>,
    metrics: Arc<QueueMetrics>,
}

#[derive(Debug, Default)]
struct QueueMetrics {
    accepted: AtomicU64,
    rejected: AtomicU64,
    completed: AtomicU64,
    cancelled: AtomicU64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum EnqueueError<T> {
    Full { item: T },
    Closed { item: T },
}

impl<T> BoundedWorkQueue<T> {
    pub fn try_enqueue(&self, item: T) -> Result<(), EnqueueError<T>> {
        match self.sender.try_reserve() {
            Ok(permit) => {
                self.depth.fetch_add(1, Ordering::Relaxed);
                self.metrics.accepted.fetch_add(1, Ordering::Relaxed);
                permit.send(item);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(())) => {
                self.metrics.rejected.fetch_add(1, Ordering::Relaxed);
                Err(EnqueueError::Full { item })
            }
            Err(mpsc::error::TrySendError::Closed(())) => {
                self.metrics.rejected.fetch_add(1, Ordering::Relaxed);
                Err(EnqueueError::Closed { item })
            }
        }
    }

    pub async fn enqueue(&self, item: T) -> Result<(), EnqueueError<T>> {
        match self.sender.reserve().await {
            Ok(permit) => {
                self.depth.fetch_add(1, Ordering::Relaxed);
                self.metrics.accepted.fetch_add(1, Ordering::Relaxed);
                permit.send(item);
                Ok(())
            }
            Err(_) => {
                self.metrics.rejected.fetch_add(1, Ordering::Relaxed);
                Err(EnqueueError::Closed { item })
            }
        }
    }

    pub fn stats(&self) -> QueueStats {
        queue_stats(self.kind, self.capacity, &self.depth, &self.metrics)
    }
}

impl<T> Clone for BoundedWorkQueue<T> {
    fn clone(&self) -> Self {
        Self {
            kind: self.kind,
            capacity: self.capacity,
            sender: self.sender.clone(),
            depth: Arc::clone(&self.depth),
            metrics: Arc::clone(&self.metrics),
        }
    }
}

impl<T> WorkReceiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        let item = self.receiver.recv().await?;
        decrement_depth(&self.depth);
        Some(item)
    }

    pub fn close(&mut self) {
        self.receiver.close();
    }

    pub fn mark_completed(&self) {
        self.metrics.completed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn mark_cancelled(&self) {
        self.metrics.cancelled.fetch_add(1, Ordering::Relaxed);
    }

    pub fn stats(&self) -> QueueStats {
        queue_stats(self.kind, self.capacity, &self.depth, &self.metrics)
    }
}

fn decrement_depth(depth: &AtomicUsize) {
    if depth
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_sub(1)
        })
        .is_err()
    {}
}

pub fn bounded_work_queue<T>(
    kind: QueueKind,
    capacity: NonZeroUsize,
) -> (BoundedWorkQueue<T>, WorkReceiver<T>) {
    let (sender, receiver) = mpsc::channel(capacity.get());
    let depth = Arc::new(AtomicUsize::new(0));
    let metrics = Arc::new(QueueMetrics::default());
    (
        BoundedWorkQueue {
            kind,
            capacity: capacity.get(),
            sender,
            depth: Arc::clone(&depth),
            metrics: Arc::clone(&metrics),
        },
        WorkReceiver {
            kind,
            capacity: capacity.get(),
            receiver,
            depth,
            metrics,
        },
    )
}

fn queue_stats(
    kind: QueueKind,
    capacity: usize,
    depth: &AtomicUsize,
    metrics: &QueueMetrics,
) -> QueueStats {
    QueueStats {
        kind,
        capacity,
        depth: depth.load(Ordering::Relaxed),
        accepted: metrics.accepted.load(Ordering::Relaxed),
        rejected: metrics.rejected.load(Ordering::Relaxed),
        completed: metrics.completed.load(Ordering::Relaxed),
        cancelled: metrics.cancelled.load(Ordering::Relaxed),
    }
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or(NonZeroUsize::MIN)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bounded_queue_applies_backpressure_and_metrics() {
        let (queue, mut receiver) = bounded_work_queue::<u32>(QueueKind::Search, nonzero(1));

        queue.try_enqueue(1).unwrap();
        let rejected = queue.try_enqueue(2).unwrap_err();
        assert_eq!(EnqueueError::Full { item: 2 }, rejected);

        assert_eq!(
            QueueStats {
                kind: QueueKind::Search,
                capacity: 1,
                depth: 1,
                accepted: 1,
                rejected: 1,
                completed: 0,
                cancelled: 0,
            },
            queue.stats()
        );
        assert_eq!(Some(1), receiver.recv().await);
        receiver.mark_completed();
        assert_eq!(0, queue.stats().depth);
        assert_eq!(1, queue.stats().completed);
        assert_eq!(0, queue.stats().cancelled);
        receiver.mark_cancelled();
        assert_eq!(1, queue.stats().cancelled);
    }

    #[tokio::test]
    async fn queue_close_cancels_future_work() {
        let (queue, mut receiver) = bounded_work_queue::<u32>(QueueKind::Injection, nonzero(2));

        receiver.close();
        let rejected = queue.enqueue(1).await.unwrap_err();

        assert_eq!(EnqueueError::Closed { item: 1 }, rejected);
        assert_eq!(1, queue.stats().rejected);
    }

    #[tokio::test]
    async fn try_enqueue_closed_keeps_metrics_and_depth_stable() {
        let (queue, mut receiver) = bounded_work_queue::<u32>(QueueKind::Search, nonzero(1));

        receiver.close();
        let rejected = queue.try_enqueue(1).unwrap_err();

        assert_eq!(EnqueueError::Closed { item: 1 }, rejected);
        assert_eq!(
            QueueStats {
                kind: QueueKind::Search,
                capacity: 1,
                depth: 0,
                accepted: 0,
                rejected: 1,
                completed: 0,
                cancelled: 0,
            },
            queue.stats()
        );
    }

    #[tokio::test]
    async fn queue_depth_stays_correct_when_waiting_send_is_received_immediately() {
        let (queue, mut receiver) = bounded_work_queue::<u32>(QueueKind::Search, nonzero(1));
        queue.try_enqueue(1).unwrap();
        let producer = tokio::spawn({
            let queue = queue.clone();
            async move { queue.enqueue(2).await }
        });

        tokio::task::yield_now().await;
        assert_eq!(Some(1), receiver.recv().await);
        assert_eq!(Some(2), receiver.recv().await);
        assert_eq!(0, queue.stats().depth);
        assert_eq!(Ok(()), producer.await.unwrap());

        assert_eq!(0, queue.stats().depth);
        assert_eq!(2, queue.stats().accepted);
    }

    #[tokio::test]
    async fn queue_depth_decrement_saturates_stale_zero_depth() {
        let (sender, receiver) = mpsc::channel(1);
        let depth = Arc::new(AtomicUsize::new(0));
        let metrics = Arc::new(QueueMetrics::default());
        let mut receiver = WorkReceiver {
            kind: QueueKind::Search,
            capacity: 1,
            receiver,
            depth: Arc::clone(&depth),
            metrics,
        };
        let (observed_sender, observed_receiver) = tokio::sync::oneshot::channel();
        let consumer = tokio::spawn(async move {
            assert_eq!(Some(7), receiver.recv().await);
            observed_sender.send(receiver.stats().depth).unwrap();
        });

        sender.try_send(7).unwrap();
        let observed_depth = observed_receiver.await.unwrap();
        consumer.await.unwrap();

        assert_eq!(0, observed_depth);
        assert_eq!(0, depth.load(Ordering::Relaxed));
    }

    #[test]
    fn runtime_queue_config_has_explicit_limits_for_work_types() {
        let config = RuntimeQueueConfig::default();

        assert_eq!(nonzero(1_000), config.limit_for(QueueKind::Announcement));
        assert_eq!(nonzero(100), config.limit_for(QueueKind::Search));
        assert_eq!(nonzero(100), config.limit_for(QueueKind::Injection));
        assert_eq!(nonzero(500), config.limit_for(QueueKind::Notification));
        assert_eq!(nonzero(50), config.limit_for(QueueKind::Indexing));
    }
}
