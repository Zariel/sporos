use super::*;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct InventoryActivityInput {
    pub(super) request: InventoryWorkflowRequest,
    pub(super) started_at_ms: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct InventoryActivityOutput {
    pub(super) scanned_items: usize,
    pub(super) persisted_items: usize,
    pub(super) pruned_items: u64,
    pub(super) scan_failure_count: usize,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InventoryCompletionWaiter {
    pub workflow_id: String,
    pub event_name: WorkflowEventName,
    pub required_after_ms: i64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct InventoryCompletionWaitRegistration {
    pub inserted: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct InventoryCompletionEvent {
    pub inventory_kind: InventoryRefreshKind,
    pub source_workflow_id: String,
    pub completed_at_ms: i64,
    pub scanned_items: usize,
    pub persisted_items: usize,
    pub pruned_items: u64,
}

impl InventoryCompletionEvent {
    fn event_name(&self) -> WorkflowEventName {
        match self.inventory_kind {
            InventoryRefreshKind::MediaFull | InventoryRefreshKind::MediaChanged => {
                WorkflowEventName::MediaInventoryCompleted
            }
            InventoryRefreshKind::Client => WorkflowEventName::ClientInventoryCompleted,
        }
    }

    fn to_record(&self) -> WorkflowInventoryCompletionRecord {
        WorkflowInventoryCompletionRecord {
            event_name: self.event_name().as_str().to_owned(),
            source_workflow_id: self.source_workflow_id.clone(),
            completed_at_ms: self.completed_at_ms,
            inventory_kind: inventory_refresh_kind_key(self.inventory_kind).to_owned(),
            scanned_items: self.scanned_items,
            persisted_items: self.persisted_items,
            pruned_items: self.pruned_items,
        }
    }

    fn from_record(record: &WorkflowInventoryCompletionRecord) -> Result<Self, String> {
        let inventory_kind = inventory_refresh_kind_from_key(&record.inventory_kind)?;
        Ok(Self {
            inventory_kind,
            source_workflow_id: record.source_workflow_id.clone(),
            completed_at_ms: record.completed_at_ms,
            scanned_items: record.scanned_items,
            persisted_items: record.persisted_items,
            pruned_items: record.pruned_items,
        })
    }
}

pub(super) fn inventory_refresh_kind_key(kind: InventoryRefreshKind) -> &'static str {
    match kind {
        InventoryRefreshKind::MediaFull => "media_full",
        InventoryRefreshKind::MediaChanged => "media_changed",
        InventoryRefreshKind::Client => "client",
    }
}

pub(super) fn inventory_refresh_kind_from_key(value: &str) -> Result<InventoryRefreshKind, String> {
    match value {
        "media_full" => Ok(InventoryRefreshKind::MediaFull),
        "media_changed" => Ok(InventoryRefreshKind::MediaChanged),
        "client" => Ok(InventoryRefreshKind::Client),
        _ => Err(format!("unknown inventory refresh kind `{value}`")),
    }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct InventoryCompletionFanout {
    pub waiters: usize,
    pub delivered: usize,
    pub failed: usize,
}

#[derive(Clone)]
pub struct InventoryCompletionEventBridge {
    pub(super) store: Arc<dyn Provider>,
    pub(super) repository: Option<Repository>,
    pub(super) waiters: Arc<Mutex<InventoryCompletionWaiters>>,
}

#[derive(Debug, Default)]
pub(super) struct InventoryCompletionWaiters {
    pub(super) by_event: BTreeMap<String, BTreeMap<String, i64>>,
}

impl InventoryCompletionEventBridge {
    pub(super) fn new(store: Arc<dyn Provider>, repository: Option<Repository>) -> Self {
        Self {
            store,
            repository,
            waiters: Arc::new(Mutex::new(InventoryCompletionWaiters::default())),
        }
    }

    pub(super) async fn register_waiter(
        &self,
        waiter: InventoryCompletionWaiter,
    ) -> Result<InventoryCompletionWaitRegistration, String> {
        let event_name = inventory_completion_event_name(waiter.event_name)?;
        if waiter.workflow_id.is_empty() {
            return Err("inventory completion waiter workflow id must not be empty".to_owned());
        }
        if let Some(repository) = &self.repository {
            let inserted = repository
                .record_workflow_inventory_waiter(
                    event_name,
                    &waiter.workflow_id,
                    waiter.required_after_ms,
                    unix_time_ms(),
                )
                .await
                .map_err(|error| error.to_string())?;
            return Ok(InventoryCompletionWaitRegistration { inserted });
        }
        self.register_memory_waiter(event_name, waiter)
    }

    fn register_memory_waiter(
        &self,
        event_name: &str,
        waiter: InventoryCompletionWaiter,
    ) -> Result<InventoryCompletionWaitRegistration, String> {
        let mut waiters = self
            .waiters
            .lock()
            .map_err(|_error| "inventory completion waiter registry is poisoned".to_owned())?;
        let event_waiters = waiters.by_event.entry(event_name.to_owned()).or_default();
        let inserted = event_waiters
            .insert(waiter.workflow_id, waiter.required_after_ms)
            .is_none();
        Ok(InventoryCompletionWaitRegistration { inserted })
    }

    pub(super) async fn publish_completion(
        &self,
        event: &InventoryCompletionEvent,
    ) -> Result<InventoryCompletionFanout, String> {
        self.record_completion(event).await?;
        let summary = self.drain_completion_event(event).await;
        if let Ok(fanout) = summary.as_ref()
            && self.completion_can_be_deleted(event, *fanout).await?
        {
            self.delete_persisted_completion(event).await?;
        }
        summary
    }

    pub(super) async fn drain_persisted_completions(
        &self,
    ) -> Result<InventoryCompletionFanout, String> {
        let Some(repository) = &self.repository else {
            return Ok(InventoryCompletionFanout::default());
        };
        let completions = repository
            .workflow_inventory_completions(INVENTORY_COMPLETION_FANOUT_LIMIT)
            .await
            .map_err(|error| format!("read persisted inventory completions failed: {error}"))?;
        let mut total = InventoryCompletionFanout::default();
        for completion in completions {
            let event = InventoryCompletionEvent::from_record(&completion)?;
            let summary = self.drain_completion_event(&event).await?;
            total.waiters += summary.waiters;
            total.delivered += summary.delivered;
            total.failed += summary.failed;
            if self.completion_can_be_deleted(&event, summary).await? {
                self.delete_persisted_completion(&event).await?;
            }
        }
        Ok(total)
    }

    async fn record_completion(&self, event: &InventoryCompletionEvent) -> Result<(), String> {
        let Some(repository) = &self.repository else {
            return Ok(());
        };
        repository
            .record_workflow_inventory_completion(&event.to_record(), unix_time_ms())
            .await
            .map_err(|error| {
                format!(
                    "record inventory completion `{}` failed: {error}",
                    event.source_workflow_id
                )
            })?;
        Ok(())
    }

    async fn delete_persisted_completion(
        &self,
        event: &InventoryCompletionEvent,
    ) -> Result<(), String> {
        let Some(repository) = &self.repository else {
            return Ok(());
        };
        repository
            .delete_workflow_inventory_completion(
                event.event_name().as_str(),
                &event.source_workflow_id,
                event.completed_at_ms,
            )
            .await
            .map_err(|error| {
                format!(
                    "delete inventory completion `{}` failed: {error}",
                    event.source_workflow_id
                )
            })?;
        Ok(())
    }

    async fn completion_can_be_deleted(
        &self,
        event: &InventoryCompletionEvent,
        summary: InventoryCompletionFanout,
    ) -> Result<bool, String> {
        if summary.waiters > 0 {
            return Ok(true);
        }
        let Some(repository) = &self.repository else {
            return Ok(true);
        };
        let due_count = repository
            .workflow_inventory_waiters_due_count(
                event.event_name().as_str(),
                event.completed_at_ms,
            )
            .await
            .map_err(|error| {
                format!(
                    "count waiters for inventory completion `{}` failed: {error}",
                    event.source_workflow_id
                )
            })?;
        Ok(due_count == 0)
    }

    async fn drain_completion_event(
        &self,
        event: &InventoryCompletionEvent,
    ) -> Result<InventoryCompletionFanout, String> {
        let event_name = event.event_name().as_str().to_owned();
        let lease_owner = format!(
            "inventory-completion:{}:{}",
            event.source_workflow_id, event.completed_at_ms
        );
        let mut summary = InventoryCompletionFanout::default();
        let mut cleanup_conflict = false;
        loop {
            let ready = self
                .ready_waiters(&event_name, event.completed_at_ms, &lease_owner)
                .await?;
            let batch_len = ready.len();
            if ready.is_empty() {
                break;
            }
            summary.waiters += batch_len;
            let mut batch_failed = false;
            let mut cleanup_failed = false;
            for waiter in ready {
                let client = Client::new(Arc::clone(&self.store));
                match self
                    .enqueue_completion_if_target_is_running(&client, &waiter, &event_name, event)
                    .await
                {
                    Ok(()) => {
                        summary.delivered += 1;
                        cleanup_failed |=
                            !self.remove_delivered_waiter(&event_name, &waiter).await?;
                    }
                    Err(error) => {
                        summary.failed += 1;
                        batch_failed = true;
                        self.release_waiter_after_delivery_failure(
                            &event_name,
                            &waiter,
                            &error.to_string(),
                        )
                        .await?;
                        tracing::warn!(
                            workflow_id = waiter.workflow_id,
                            event_name,
                            error = %error,
                            "inventory completion event delivery failed"
                        );
                    }
                }
            }
            let maybe_more_repository_waiters = self.repository.is_some()
                && batch_len == usize::from(INVENTORY_COMPLETION_FANOUT_LIMIT);
            cleanup_conflict |= cleanup_failed;
            if !maybe_more_repository_waiters || batch_failed || cleanup_failed {
                break;
            }
        }
        if cleanup_conflict {
            Err("inventory completion fanout could not confirm delivered waiter cleanup".to_owned())
        } else if summary.failed > 0 {
            Err(format!(
                "inventory completion fanout failed for {} of {} waiters",
                summary.failed, summary.waiters
            ))
        } else {
            Ok(summary)
        }
    }

    async fn enqueue_completion_if_target_is_running(
        &self,
        client: &Client,
        waiter: &InventoryCompletionReadyWaiter,
        event_name: &str,
        event: &InventoryCompletionEvent,
    ) -> Result<(), String> {
        match client
            .get_orchestration_status(&waiter.workflow_id)
            .await
            .map_err(|error| error.to_string())?
        {
            OrchestrationStatus::Running { .. } => client
                .enqueue_event_typed(&waiter.workflow_id, event_name, event)
                .await
                .map_err(|error| error.to_string()),
            OrchestrationStatus::NotFound => Err(format!(
                "target workflow `{}` is not found",
                waiter.workflow_id
            )),
            OrchestrationStatus::Completed { .. } => Err(format!(
                "target workflow `{}` is already completed",
                waiter.workflow_id
            )),
            OrchestrationStatus::Failed { details, .. } => Err(format!(
                "target workflow `{}` is failed: {}",
                waiter.workflow_id,
                details.display_message()
            )),
        }
    }

    async fn ready_waiters(
        &self,
        event_name: &str,
        completed_at_ms: i64,
        lease_owner: &str,
    ) -> Result<Vec<InventoryCompletionReadyWaiter>, String> {
        let mut ready = BTreeMap::<String, InventoryCompletionReadyWaiter>::new();
        if let Some(repository) = &self.repository {
            let now_ms = unix_time_ms();
            let rows = repository
                .claim_workflow_inventory_waiters_ready(
                    event_name,
                    completed_at_ms,
                    now_ms,
                    lease_owner,
                    now_ms.saturating_add(INVENTORY_COMPLETION_LEASE_MS),
                    INVENTORY_COMPLETION_FANOUT_LIMIT,
                )
                .await
                .map_err(|error| {
                    format!("claim inventory completion waiters for `{event_name}` failed: {error}")
                })?;
            for row in rows {
                ready.insert(
                    row.workflow_id.clone(),
                    InventoryCompletionReadyWaiter::from_repository(row, lease_owner.to_owned()),
                );
            }
        }
        for workflow_id in self.ready_memory_waiters(event_name, completed_at_ms) {
            ready
                .entry(workflow_id.clone())
                .and_modify(|waiter| waiter.memory = true)
                .or_insert_with(|| InventoryCompletionReadyWaiter::from_memory(workflow_id));
        }
        Ok(ready.into_values().collect())
    }

    async fn release_waiter_after_delivery_failure(
        &self,
        event_name: &str,
        waiter: &InventoryCompletionReadyWaiter,
        error: &str,
    ) -> Result<(), String> {
        if waiter.repository
            && let Some(repository) = &self.repository
        {
            let lease_owner = waiter.lease_owner.as_deref().unwrap_or_default();
            repository
                .release_workflow_inventory_waiter(
                    event_name,
                    &waiter.workflow_id,
                    lease_owner,
                    error,
                )
                .await
                .map_err(|error| {
                    format!(
                        "release inventory completion waiter `{}` failed: {error}",
                        waiter.workflow_id
                    )
                })?;
        }
        Ok(())
    }

    fn ready_memory_waiters(&self, event_name: &str, completed_at_ms: i64) -> Vec<String> {
        let Ok(waiters) = self.waiters.lock() else {
            return Vec::new();
        };
        waiters
            .by_event
            .get(event_name)
            .into_iter()
            .flat_map(|event_waiters| event_waiters.iter())
            .filter(|(_workflow_id, required_after_ms)| completed_at_ms >= **required_after_ms)
            .map(|(workflow_id, _required_after_ms)| workflow_id.clone())
            .collect()
    }

    async fn remove_delivered_waiter(
        &self,
        event_name: &str,
        waiter: &InventoryCompletionReadyWaiter,
    ) -> Result<bool, String> {
        let mut removed = true;
        if waiter.repository
            && let Some(repository) = &self.repository
        {
            let lease_owner = waiter.lease_owner.as_deref().unwrap_or_default();
            let deleted = repository
                .delete_claimed_workflow_inventory_waiter(
                    event_name,
                    &waiter.workflow_id,
                    lease_owner,
                )
                .await
                .map_err(|error| {
                    format!(
                        "delete delivered inventory completion waiter `{}` failed: {error}",
                        waiter.workflow_id
                    )
                })?;
            removed &= deleted;
        }
        if waiter.memory {
            self.remove_memory_waiter(event_name, &waiter.workflow_id);
        }
        Ok(removed)
    }

    fn remove_memory_waiter(&self, event_name: &str, workflow_id: &str) {
        let Ok(mut waiters) = self.waiters.lock() else {
            return;
        };
        if let Some(event_waiters) = waiters.by_event.get_mut(event_name) {
            event_waiters.remove(workflow_id);
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(super) struct InventoryCompletionReadyWaiter {
    pub(super) workflow_id: String,
    pub(super) repository: bool,
    pub(super) memory: bool,
    pub(super) lease_owner: Option<String>,
}

impl InventoryCompletionReadyWaiter {
    fn from_repository(row: WorkflowInventoryWaiterRecord, lease_owner: String) -> Self {
        Self {
            workflow_id: row.workflow_id,
            repository: true,
            memory: false,
            lease_owner: Some(lease_owner),
        }
    }

    fn from_memory(workflow_id: String) -> Self {
        Self {
            workflow_id,
            repository: false,
            memory: true,
            lease_owner: None,
        }
    }
}

pub(super) fn inventory_completion_event_name(
    event_name: WorkflowEventName,
) -> Result<&'static str, String> {
    match event_name {
        WorkflowEventName::MediaInventoryCompleted
        | WorkflowEventName::ClientInventoryCompleted => Ok(event_name.as_str()),
        _ => Err(format!(
            "workflow event `{}` is not an inventory completion event",
            event_name.as_str()
        )),
    }
}

impl fmt::Debug for InventoryCompletionEventBridge {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InventoryCompletionEventBridge")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) struct InventoryProjectionRecord<'a> {
    pub(super) workflow_id: &'a str,
    pub(super) public_id: &'a str,
    pub(super) state: WorkflowState,
    pub(super) reason: WorkflowReason,
    pub(super) next_action: Option<&'a str>,
    pub(super) started_at_ms: i64,
    pub(super) updated_at_ms: i64,
    pub(super) finished_at_ms: Option<i64>,
    pub(super) blocked_dependency_name: Option<&'a str>,
}
