use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::Limits;

#[derive(Clone)]
pub(crate) struct ExecutionLimits {
    candidate: Arc<Semaphore>,
    search: Arc<Semaphore>,
    indexer: Arc<Semaphore>,
    filesystem: Arc<Semaphore>,
}

impl ExecutionLimits {
    pub(crate) fn new(limits: &Limits) -> Self {
        Self {
            candidate: Arc::new(Semaphore::new(limits.max_candidate_workflows)),
            search: Arc::new(Semaphore::new(limits.max_search_workflows)),
            indexer: Arc::new(Semaphore::new(limits.max_indexer_requests)),
            filesystem: Arc::new(Semaphore::new(limits.max_filesystem_operations)),
        }
    }

    pub(crate) fn candidate(&self) -> Arc<Semaphore> {
        Arc::clone(&self.candidate)
    }

    pub(crate) fn search(&self) -> Arc<Semaphore> {
        Arc::clone(&self.search)
    }

    pub(crate) fn indexer(&self) -> Arc<Semaphore> {
        Arc::clone(&self.indexer)
    }

    pub(crate) fn filesystem(&self) -> Arc<Semaphore> {
        Arc::clone(&self.filesystem)
    }
}

pub(crate) async fn permit(semaphore: &Arc<Semaphore>) -> OwnedSemaphorePermit {
    Arc::clone(semaphore)
        .acquire_owned()
        .await
        .expect("execution semaphore remains open for the process lifetime")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn each_configured_class_bounds_observable_concurrency() {
        let limits = ExecutionLimits::new(&Limits {
            max_candidate_workflows: 1,
            max_search_workflows: 1,
            max_indexer_requests: 1,
            max_filesystem_operations: 1,
            ..Limits::default()
        });
        for semaphore in [
            limits.candidate(),
            limits.search(),
            limits.indexer(),
            limits.filesystem(),
        ] {
            let held = permit(&semaphore).await;
            assert!(
                tokio::time::timeout(Duration::from_millis(10), permit(&semaphore))
                    .await
                    .is_err()
            );
            drop(held);
            assert!(
                tokio::time::timeout(Duration::from_millis(10), permit(&semaphore))
                    .await
                    .is_ok()
            );
        }
    }
}
