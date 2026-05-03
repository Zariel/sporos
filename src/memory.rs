//! Memory budgets for large-inventory compatibility work.

/// Expected local torrent inventory for routine profiling.
pub const CLIENT_INVENTORY_BASELINE_TORRENTS: usize = 10_000;

/// Design target for production inventory paths.
pub const CLIENT_INVENTORY_TARGET_TORRENTS: usize = 100_000;

/// Budget for one large collection path.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct CollectionBudget {
    /// Collection or workflow name.
    pub name: &'static str,
    /// Routine local scale that should be easy to profile.
    pub baseline_items: usize,
    /// Production scale the design should handle.
    pub target_items: usize,
    /// Target peak bytes held per item after parsing or indexing.
    pub peak_bytes_per_item: usize,
}

/// Regression gate for one memory-sensitive workflow.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct MemoryRegressionGate {
    /// Workflow covered by the gate.
    pub name: &'static str,
    /// Routine local scale exercised by benchmarks or focused tests.
    pub baseline_items: usize,
    /// Production scale the implementation is designed around.
    pub target_items: usize,
    /// Maximum items intentionally retained in one active batch.
    pub max_active_items: usize,
}

/// Deterministic allocation budget enforced by `tests/memory_peak_gates.rs`.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct AllocationRegressionGate {
    /// Workflow covered by the measured allocation gate.
    pub name: &'static str,
    /// Stable fixture scale used by the gate.
    pub fixture_items: usize,
    /// Maximum live heap bytes allowed while fixture output is retained.
    pub max_live_bytes: usize,
    /// Maximum total heap bytes allocated while processing the fixture.
    pub max_total_allocated_bytes: usize,
}

/// Initial budgets for memory-sensitive runtime paths.
pub const COLLECTION_BUDGETS: &[CollectionBudget] = &[
    CollectionBudget {
        name: "client torrent inventory",
        baseline_items: CLIENT_INVENTORY_BASELINE_TORRENTS,
        target_items: CLIENT_INVENTORY_TARGET_TORRENTS,
        peak_bytes_per_item: 512,
    },
    CollectionBudget {
        name: "data-dir walk entries",
        baseline_items: 100_000,
        target_items: 1_000_000,
        peak_bytes_per_item: 256,
    },
    CollectionBudget {
        name: "RSS candidates",
        baseline_items: 1_000,
        target_items: 10_000,
        peak_bytes_per_item: 768,
    },
    CollectionBudget {
        name: "search candidates",
        baseline_items: 10_000,
        target_items: 100_000,
        peak_bytes_per_item: 768,
    },
];

/// Workflows that must stay visible in memory regression checks.
pub const MEMORY_REGRESSION_GATES: &[MemoryRegressionGate] = &[
    MemoryRegressionGate {
        name: "torrent-dir listing",
        baseline_items: 10_000,
        target_items: 100_000,
        max_active_items: 1,
    },
    MemoryRegressionGate {
        name: "data-dir traversal",
        baseline_items: 100_000,
        target_items: 1_000_000,
        max_active_items: 1,
    },
    MemoryRegressionGate {
        name: "client inventory refresh",
        baseline_items: CLIENT_INVENTORY_BASELINE_TORRENTS,
        target_items: CLIENT_INVENTORY_TARGET_TORRENTS,
        max_active_items: crate::clients::CLIENT_INVENTORY_PAGE_SIZE,
    },
    MemoryRegressionGate {
        name: "qBittorrent file-list refresh",
        baseline_items: CLIENT_INVENTORY_BASELINE_TORRENTS,
        target_items: CLIENT_INVENTORY_TARGET_TORRENTS,
        max_active_items: crate::clients::QB_TORRENT_FILES_CONCURRENCY_LIMIT,
    },
    MemoryRegressionGate {
        name: "RSS candidate parsing",
        baseline_items: 1_000,
        target_items: 10_000,
        max_active_items: 1_000,
    },
    MemoryRegressionGate {
        name: "candidate assessment",
        baseline_items: 10_000,
        target_items: 100_000,
        max_active_items: 1,
    },
    MemoryRegressionGate {
        name: "injection and linking",
        baseline_items: 10_000,
        target_items: 100_000,
        max_active_items: 1,
    },
    MemoryRegressionGate {
        name: "cleanup maintenance",
        baseline_items: CLIENT_INVENTORY_BASELINE_TORRENTS,
        target_items: CLIENT_INVENTORY_TARGET_TORRENTS,
        max_active_items: crate::clients::CLIENT_INVENTORY_PAGE_SIZE,
    },
];

/// Allocation gates run in CI with an instrumented global allocator.
pub const ALLOCATION_REGRESSION_GATES: &[AllocationRegressionGate] = &[
    AllocationRegressionGate {
        name: "client inventory to searchees",
        fixture_items: CLIENT_INVENTORY_BASELINE_TORRENTS,
        max_live_bytes: 12 * 1024 * 1024,
        max_total_allocated_bytes: 20 * 1024 * 1024,
    },
    AllocationRegressionGate {
        name: "RSS candidate parsing",
        fixture_items: 1_000,
        max_live_bytes: 2 * 1024 * 1024,
        max_total_allocated_bytes: 4 * 1024 * 1024,
    },
    AllocationRegressionGate {
        name: "candidate assessment",
        fixture_items: 10_000,
        max_live_bytes: 128 * 1024,
        max_total_allocated_bytes: 5 * 1024 * 1024,
    },
];

#[cfg(test)]
mod tests {
    use super::{
        ALLOCATION_REGRESSION_GATES, CLIENT_INVENTORY_BASELINE_TORRENTS,
        CLIENT_INVENTORY_TARGET_TORRENTS, COLLECTION_BUDGETS, MEMORY_REGRESSION_GATES,
    };

    #[test]
    fn large_inventory_budget_matches_project_scale() {
        assert_eq!(CLIENT_INVENTORY_BASELINE_TORRENTS, 10_000);
        assert_eq!(CLIENT_INVENTORY_TARGET_TORRENTS, 100_000);
    }

    #[test]
    fn every_collection_budget_has_growth_headroom() {
        for budget in COLLECTION_BUDGETS {
            assert!(budget.target_items >= budget.baseline_items);
            assert!(budget.peak_bytes_per_item > 0);
        }
    }

    #[test]
    fn regression_gates_cover_release_memory_paths() {
        let required = [
            "torrent-dir listing",
            "data-dir traversal",
            "client inventory refresh",
            "qBittorrent file-list refresh",
            "RSS candidate parsing",
            "candidate assessment",
            "injection and linking",
            "cleanup maintenance",
        ];
        for name in required {
            assert!(
                MEMORY_REGRESSION_GATES.iter().any(|gate| gate.name == name),
                "missing memory regression gate for {name}"
            );
        }
    }

    #[test]
    fn regression_gates_keep_active_batches_bounded() {
        for gate in MEMORY_REGRESSION_GATES {
            assert!(gate.target_items >= gate.baseline_items);
            assert!(gate.max_active_items > 0);
            assert!(gate.max_active_items <= gate.baseline_items);
        }
        let qb_files = MEMORY_REGRESSION_GATES
            .iter()
            .find(|gate| gate.name == "qBittorrent file-list refresh")
            .expect("qBittorrent file-list gate");
        assert_eq!(qb_files.max_active_items, 1);
    }

    #[test]
    fn allocation_gates_cover_measured_release_paths() {
        let required = [
            "client inventory to searchees",
            "RSS candidate parsing",
            "candidate assessment",
        ];
        for name in required {
            assert!(
                ALLOCATION_REGRESSION_GATES
                    .iter()
                    .any(|gate| gate.name == name),
                "missing allocation regression gate for {name}"
            );
        }
        for gate in ALLOCATION_REGRESSION_GATES {
            assert!(gate.fixture_items > 0);
            assert!(gate.max_live_bytes > 0);
            assert!(gate.max_total_allocated_bytes >= gate.max_live_bytes);
        }
    }
}
