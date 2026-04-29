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

/// Initial budgets for memory-sensitive rebuild paths.
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

#[cfg(test)]
mod tests {
    use super::{
        CLIENT_INVENTORY_BASELINE_TORRENTS, CLIENT_INVENTORY_TARGET_TORRENTS, COLLECTION_BUDGETS,
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
}
