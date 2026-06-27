use super::*;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct WorkflowSupervisorInput {
    pub(super) kind: WorkflowKind,
    pub(super) public_id: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct WorkflowSupervisorOutput {
    pub(super) kind: WorkflowKind,
    pub(super) public_id: String,
    pub(super) state: WorkflowState,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct WorkflowShellActivityInput {
    pub(super) activity: ActivityKind,
    pub(super) workflow_id: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct WorkflowShellActivityOutput {
    pub(super) activity: ActivityKind,
    pub(super) accepted: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct ScheduledJobManualRequest {
    pub(super) requested_at_ms: i64,
    pub(super) forced: bool,
    pub(super) claimed_scheduled_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct ScheduledJobClaimActivityInput {
    pub(super) job_name: String,
    pub(super) now_ms: i64,
    pub(super) manual: Option<ScheduledJobManualRequest>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct ScheduledJobClaimActivityOutput {
    pub(super) job_name: String,
    pub(super) scheduled_at_ms: Option<i64>,
    pub(super) next_run_at_ms: Option<i64>,
    pub(super) coalesced: bool,
    pub(super) backing_off: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct ScheduledJobCompleteActivityInput {
    pub(super) job_name: String,
    pub(super) scheduled_at_ms: i64,
    pub(super) succeeded: bool,
    pub(super) error: Option<String>,
    pub(super) finished_at_ms: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct ScheduledJobRunActivityInput {
    pub(super) job_name: String,
    pub(super) scheduled_at_ms: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct ScheduledJobRunActivityOutput {
    pub(super) succeeded: bool,
    pub(super) error: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) struct WorkflowRuntimeActivities {
    pub(super) repository: Repository,
    pub(super) inventory: Option<InventoryWorkflowActivities>,
    pub(super) announce: Option<AnnounceWorkflowActivities>,
    pub(super) scheduled_jobs: Option<ScheduledJobWorkflowActivities>,
    pub(super) search: Option<SearchWorkflowActivities>,
    pub(super) saved_retry: Option<SavedRetryWorkflowActivities>,
}

impl WorkflowRuntimeActivities {
    pub(super) fn with_completion_event_bridge(
        mut self,
        completion_events: InventoryCompletionEventBridge,
    ) -> Self {
        if let Some(inventory) = self.inventory.take() {
            self.inventory =
                Some(inventory.with_completion_event_bridge(completion_events.clone()));
        }
        if let Some(announce) = self.announce.take() {
            self.announce = Some(announce.with_completion_event_bridge(completion_events));
        }
        self
    }
}

#[derive(Debug, Clone)]
pub struct ScheduledJobWorkflowActivities {
    pub(super) scheduler: PersistedScheduler,
    pub(super) shutdown: ShutdownSignal,
    pub(super) state: ScheduledJobStateHandle,
    pub(super) inventory: Option<InventoryWorkflowActivities>,
    pub(super) active_inventory_refreshes: Option<Arc<Mutex<BTreeSet<String>>>>,
}

impl ScheduledJobWorkflowActivities {
    pub fn new(
        scheduler: PersistedScheduler,
        shutdown: ShutdownSignal,
        state: ScheduledJobStateHandle,
    ) -> Self {
        Self {
            scheduler,
            shutdown,
            state,
            inventory: None,
            active_inventory_refreshes: None,
        }
    }

    pub(super) fn with_inventory_runtime(
        mut self,
        inventory: InventoryWorkflowActivities,
        active_inventory_refreshes: Arc<Mutex<BTreeSet<String>>>,
    ) -> Self {
        self.inventory = Some(inventory);
        self.active_inventory_refreshes = Some(active_inventory_refreshes);
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct ScheduledJobStateHandle {
    pub(super) state: Arc<Mutex<Option<crate::runtime::app::AppState>>>,
}

impl ScheduledJobStateHandle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn bind(&self, state: crate::runtime::app::AppState) -> bool {
        let Ok(mut guard) = self.state.lock() else {
            return false;
        };
        if guard.is_some() {
            return false;
        }
        *guard = Some(state);
        true
    }

    pub(super) fn get(&self) -> Option<crate::runtime::app::AppState> {
        self.state.lock().ok().and_then(|guard| guard.clone())
    }

    pub(super) fn clear(&self) {
        if let Ok(mut guard) = self.state.lock() {
            *guard = None;
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SearchWorkflowStateHandle {
    pub(super) state: Arc<Mutex<Option<crate::runtime::app::AppState>>>,
}

impl SearchWorkflowStateHandle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn bind(&self, state: crate::runtime::app::AppState) -> bool {
        let Ok(mut guard) = self.state.lock() else {
            return false;
        };
        if guard.is_some() {
            return false;
        }
        *guard = Some(state);
        true
    }

    pub(super) fn get(&self) -> Option<crate::runtime::app::AppState> {
        self.state.lock().ok().and_then(|guard| guard.clone())
    }

    pub(super) fn clear(&self) {
        if let Ok(mut guard) = self.state.lock() {
            *guard = None;
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SavedRetryWorkflowStateHandle {
    pub(super) state: Arc<Mutex<Option<crate::runtime::app::AppState>>>,
}

impl SavedRetryWorkflowStateHandle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn bind(&self, state: crate::runtime::app::AppState) -> bool {
        let Ok(mut guard) = self.state.lock() else {
            return false;
        };
        if guard.is_some() {
            return false;
        }
        *guard = Some(state);
        true
    }

    pub(super) fn get(&self) -> Option<crate::runtime::app::AppState> {
        self.state.lock().ok().and_then(|guard| guard.clone())
    }

    pub(super) fn clear(&self) {
        if let Ok(mut guard) = self.state.lock() {
            *guard = None;
        }
    }
}

#[derive(Debug, Clone)]
pub struct SearchWorkflowActivities {
    pub(super) state: SearchWorkflowStateHandle,
    pub(super) shutdown: ShutdownSignal,
}

impl SearchWorkflowActivities {
    pub fn new(state: SearchWorkflowStateHandle, shutdown: ShutdownSignal) -> Self {
        Self { state, shutdown }
    }
}

#[derive(Debug, Clone)]
pub struct SavedRetryWorkflowActivities {
    pub(super) state: SavedRetryWorkflowStateHandle,
    pub(super) shutdown: ShutdownSignal,
}

impl SavedRetryWorkflowActivities {
    pub fn new(state: SavedRetryWorkflowStateHandle, shutdown: ShutdownSignal) -> Self {
        Self { state, shutdown }
    }
}

#[derive(Debug, Clone)]
pub struct AnnounceWorkflowActivities {
    pub(super) repository: Repository,
    pub(super) processor: AnnounceProcessor,
    pub(super) queue_config: AnnounceQueueConfig,
    pub(super) shutdown: ShutdownSignal,
    pub(super) completion_events: Option<InventoryCompletionEventBridge>,
}

impl AnnounceWorkflowActivities {
    pub fn new(
        repository: Repository,
        processor: AnnounceProcessor,
        queue_config: AnnounceQueueConfig,
        shutdown: ShutdownSignal,
    ) -> Self {
        Self {
            repository,
            processor,
            queue_config,
            shutdown,
            completion_events: None,
        }
    }

    pub(super) fn with_completion_event_bridge(
        mut self,
        completion_events: InventoryCompletionEventBridge,
    ) -> Self {
        self.completion_events = Some(completion_events);
        self
    }
}

#[derive(Debug, Clone)]
pub struct InventoryWorkflowActivities {
    pub(super) repository: Repository,
    pub(super) inventory_refresh: InventoryRefreshWorker,
    pub(super) injection_worker: InjectionWorker,
    pub(super) shutdown: ShutdownSignal,
    pub(super) failure_backoff: Duration,
    pub(super) completion_events: Option<InventoryCompletionEventBridge>,
}

impl InventoryWorkflowActivities {
    pub fn new(
        repository: Repository,
        inventory_refresh: InventoryRefreshWorker,
        injection_worker: InjectionWorker,
        shutdown: ShutdownSignal,
        failure_backoff: Duration,
    ) -> Self {
        Self {
            repository,
            inventory_refresh,
            injection_worker,
            shutdown,
            failure_backoff,
            completion_events: None,
        }
    }

    pub(super) fn with_completion_event_bridge(
        mut self,
        completion_events: InventoryCompletionEventBridge,
    ) -> Self {
        self.completion_events = Some(completion_events);
        self
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct InventoryWorkflowRequest {
    pub kind: InventoryRefreshKind,
    pub scope_hash: Option<String>,
    pub media_dirs: Vec<PathBuf>,
    pub changed_paths: Vec<PathBuf>,
    pub requested_at_ms: i64,
}

impl InventoryWorkflowRequest {
    pub fn media_full(media_dirs: Vec<PathBuf>, requested_at_ms: i64) -> Self {
        Self {
            kind: InventoryRefreshKind::MediaFull,
            scope_hash: None,
            media_dirs,
            changed_paths: Vec::new(),
            requested_at_ms,
        }
    }

    pub fn media_changed(
        media_dirs: Vec<PathBuf>,
        changed_paths: Vec<PathBuf>,
        requested_at_ms: i64,
    ) -> Self {
        let scope_hash = changed_paths_scope_hash(&changed_paths);
        Self {
            kind: InventoryRefreshKind::MediaChanged,
            scope_hash: Some(scope_hash),
            media_dirs,
            changed_paths,
            requested_at_ms,
        }
    }

    pub fn client(requested_at_ms: i64) -> Self {
        Self {
            kind: InventoryRefreshKind::Client,
            scope_hash: None,
            media_dirs: Vec::new(),
            changed_paths: Vec::new(),
            requested_at_ms,
        }
    }

    pub fn from_inventory_request(request: InventoryRefreshRequest, requested_at_ms: i64) -> Self {
        if request.changed_paths.is_empty() {
            Self::media_full(request.media_dirs, requested_at_ms)
        } else {
            Self::media_changed(request.media_dirs, request.changed_paths, requested_at_ms)
        }
    }

    pub(super) fn instance_id(
        &self,
    ) -> Result<WorkflowInstanceId, crate::runtime::workflow_contracts::WorkflowContractError> {
        WorkflowInstanceId::inventory_refresh(self.kind, self.scope_hash.as_deref())
    }

    pub(super) fn workflow_input(&self) -> InventoryRefreshWorkflowInput {
        InventoryRefreshWorkflowInput {
            kind: self.kind,
            scope_hash: self.scope_hash.clone(),
            requested_at_ms: self.requested_at_ms,
        }
    }

    pub(super) fn activity_kind(&self) -> ActivityKind {
        match self.kind {
            InventoryRefreshKind::MediaFull | InventoryRefreshKind::MediaChanged => {
                ActivityKind::InventoryScanMedia
            }
            InventoryRefreshKind::Client => ActivityKind::InventoryRefreshClient,
        }
    }

    pub(super) fn public_id(&self) -> String {
        match self.kind {
            InventoryRefreshKind::MediaFull => "media:full".to_owned(),
            InventoryRefreshKind::MediaChanged => {
                let scope_hash = self.scope_hash.as_deref().unwrap_or("unknown");
                format!("media:changed:{scope_hash}")
            }
            InventoryRefreshKind::Client => "client".to_owned(),
        }
    }

    pub(super) fn media_request(&self) -> InventoryRefreshRequest {
        if self.changed_paths.is_empty() {
            InventoryRefreshRequest::full(self.media_dirs.clone())
        } else {
            InventoryRefreshRequest::changed_paths(
                self.media_dirs.clone(),
                self.changed_paths.clone(),
            )
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InventoryWorkflowSubmission {
    pub workflow_id: String,
    pub outcome: InventoryWorkflowSubmissionOutcome,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum InventoryWorkflowSubmissionOutcome {
    Queued,
    Coalesced,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AnnounceWorkflowSubmission {
    pub workflow_id: String,
    pub outcome: AnnounceWorkflowSubmissionOutcome,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum AnnounceWorkflowSubmissionOutcome {
    Started,
    AlreadyRunning,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SearchWorkflowSubmission {
    pub workflow_id: String,
    pub outcome: SearchWorkflowSubmissionOutcome,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SearchWorkflowSubmissionOutcome {
    Started,
    AlreadyRunning,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct SavedRetryScanActivityInput {
    pub(super) requested_at_ms: i64,
    pub(super) reason: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct SavedRetryScanActivityOutput {
    pub(super) items: Vec<SavedTorrentRetryItem>,
    pub(super) interval_ms: u64,
    pub(super) failed: usize,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct SavedRetryProcessActivityInput {
    pub(super) item: SavedTorrentRetryItem,
    pub(super) requested_at_ms: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct SavedRetryFinalizeActivityInput {
    pub(super) requested_at_ms: i64,
    pub(super) summary: SavedTorrentRetrySummary,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct SavedRetryFinalizeActivityOutput {
    pub(super) summary: SavedTorrentRetrySummary,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct SearchPlanActivityInput {
    pub(super) input: SearchWorkflowInput,
    pub(super) planned_at_ms: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct SearchPlanActivityOutput {
    pub(super) planned_indexers: usize,
    pub(super) failed_indexers: usize,
    pub(super) candidate_count: usize,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct SearchCandidatePageActivityInput {
    pub(super) start_ordinal: u32,
    pub(super) limit: u16,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct SearchCandidatePageActivityOutput {
    pub(super) refs: Vec<SearchCandidateRef>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct SearchCandidateActivityInput {
    pub(super) candidate: SearchCandidateRef,
    pub(super) planned_at_ms: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct SearchFinalizeActivityInput {
    pub(super) summary: SearchWorkflowExecutionSummary,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct SearchFinalizeActivityOutput {
    pub(super) summary: SearchWorkflowExecutionSummary,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct SearchCandidateRef {
    pub(super) ordinal: u32,
}

impl From<SearchCandidateMaterialRef> for SearchCandidateRef {
    fn from(value: SearchCandidateMaterialRef) -> Self {
        Self {
            ordinal: value.ordinal,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct AnnounceActivityInput {
    pub(super) work_id: String,
    pub(super) received_at_ms: i64,
    pub(super) raw_secret_material_count: u16,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct AnnounceProcessActivityOutput {
    pub(super) state: AnnounceActivityState,
    pub(super) reason: String,
    pub(super) next_attempt_at_ms: Option<i64>,
    pub(super) retry_delay_ms: Option<u64>,
    pub(super) dependency: Option<AnnounceProjectionDependency>,
    pub(super) events: Vec<WorkflowEventName>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct AnnounceWaitActivityOutput {
    pub(super) events: Vec<WorkflowEventName>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct AnnounceProjectionDependency {
    pub(super) kind: String,
    pub(super) name: String,
}

impl From<&AnnounceDependency> for AnnounceProjectionDependency {
    fn from(value: &AnnounceDependency) -> Self {
        Self {
            kind: value.kind.as_str().to_owned(),
            name: value.name.as_str().to_owned(),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum AnnounceActivityState {
    Succeeded,
    Failed,
    WaitingInventory,
    WaitingDependency,
    Retrying,
    Released,
}

pub(super) fn announce_workflow_input(work: &AnnounceWorkItem) -> AnnounceWorkflowInput {
    AnnounceWorkflowInput {
        work_id: work.id.as_str().to_owned(),
        dedupe_hash: work.dedupe_hash.as_str().to_owned(),
        tracker: work.tracker.as_str().to_owned(),
        candidate_guid: work
            .guid
            .as_ref()
            .map(|guid| guid.as_str().to_owned())
            .unwrap_or_default(),
        candidate_title: work.title.as_str().to_owned(),
        received_at_ms: work.received_at_ms,
        expires_at_ms: work.expires_at_ms,
        fetch_material_present: work.fetch.is_some(),
        raw_secret_material_count: u16::from(work.fetch.is_some()),
    }
}
