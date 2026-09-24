//! Persisted refresh, retention, tiering, and registry-persistence coordination.
//!
//! Stage filesystem work first and hold `flush_visibility_lock` only while
//! swapping the visible persisted state.

mod budget_enforcement;
mod catalog_refresh;
mod memory_accounting;
mod post_flush;
mod registry_persistence;

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::atomic::Ordering;

use parking_lot::RwLockWriteGuard;

use super::registry_catalog;
use super::tiering::SegmentInventory;
use super::*;
use crate::engine::segment::IndexedSegment;

const POST_FLUSH_COPY_STAGE_PURPOSE: &str = "post-flush-stage-copy";
const POST_FLUSH_REWRITE_STAGE_PURPOSE: &str = "post-flush-retention-rewrite";

struct PersistedRefreshClaim<'a> {
    refresh: PersistedRefreshContext<'a>,
}

impl Drop for PersistedRefreshClaim<'_> {
    fn drop(&mut self) {
        self.refresh
            .persisted_refresh_in_progress
            .store(false, Ordering::Release);
    }
}

pub(super) struct LoadedInventoryCatalogRefresh {
    visibility_fence: PersistedCatalogVisibilityFence,
    visible_roots: BTreeSet<PathBuf>,
    tombstones: crate::engine::tombstone::TombstoneMap,
    inventory: SegmentInventory,
}

pub(super) struct PlannedKnownDirtyCatalogRefresh {
    diff: PendingPersistedSegmentDiff,
    loaded_segments: Vec<IndexedSegment>,
}

#[derive(Clone, Copy)]
pub(super) struct PersistedCatalogVisibilityFence {
    visibility_generation: u64,
}

impl PersistedCatalogVisibilityFence {
    fn matches(self, current_visibility_generation: u64) -> bool {
        self.visibility_generation == current_visibility_generation
    }
}

pub(super) struct PlannedInventoryCatalogRefresh {
    visibility_fence: PersistedCatalogVisibilityFence,
    inventory: SegmentInventory,
    loaded_segments: Vec<IndexedSegment>,
    removed_roots: Vec<PathBuf>,
    tombstones: crate::engine::tombstone::TombstoneMap,
}

pub(super) enum PlannedPersistedCatalogRefresh {
    KnownDirty(PlannedKnownDirtyCatalogRefresh),
    Inventory(PlannedInventoryCatalogRefresh),
}

pub(super) enum PersistedCatalogPublication {
    PersistedState {
        published_segment_roots: Vec<PathBuf>,
    },
    Inventory {
        inventory: SegmentInventory,
        tombstones: Option<crate::engine::tombstone::TombstoneMap>,
    },
}

pub(super) struct PersistedCatalogTransition {
    pub(super) visibility_fence: Option<PersistedCatalogVisibilityFence>,
    pub(super) loaded_segments: Vec<IndexedSegment>,
    pub(super) removed_roots: Vec<PathBuf>,
    pub(super) publication: PersistedCatalogPublication,
    pub(super) registry_catalog_sources:
        Option<Vec<registry_catalog::PersistedRegistryCatalogSource>>,
}

impl PlannedPersistedCatalogRefresh {
    pub(super) fn restore_known_dirty_diff(&self) -> Option<PendingPersistedSegmentDiff> {
        match self {
            Self::KnownDirty(planned) => Some(planned.diff.clone()),
            Self::Inventory(_) => None,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum PersistedCatalogRefreshApply {
    Applied,
    SkippedStaleVisibleState,
}

impl PersistedCatalogRefreshApply {
    pub(super) fn is_applied(self) -> bool {
        matches!(self, Self::Applied)
    }
}

#[derive(Debug)]
struct StagedSegmentPromotion {
    staging_root: PathBuf,
    final_root: PathBuf,
}

#[derive(Debug)]
struct RetiredPostFlushRoot {
    root: PathBuf,
    counts_as_expired: bool,
}

struct StagedPostFlushMaintenance {
    final_inventory: SegmentInventory,
    promotions: Vec<StagedSegmentPromotion>,
    staging_cleanup_paths: Vec<PathBuf>,
    loaded_segments: Vec<IndexedSegment>,
    removed_roots: Vec<PathBuf>,
    retired_roots: Vec<RetiredPostFlushRoot>,
    tier_moves: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MaintenanceInventorySource {
    PersistedState,
    Scanned,
}

/// Holds the visibility write fence while persisted-catalog transitions are
/// swapped in. The registry catalog sidecar of the published state is recovery
/// metadata that startup validates against the segments on disk, so it is written
/// only after the fence is released: [`Self::finish`] reports a failure to the
/// caller, and dropping the guard without finishing still writes it and logs a
/// failure.
pub(super) struct PersistedCatalogPublicationGuard<'a> {
    storage: &'a ChunkStorage,
    visibility_guard: Option<RwLockWriteGuard<'a, ()>>,
    registry_catalog_sources: Option<Vec<registry_catalog::PersistedRegistryCatalogSource>>,
}

impl<'a> PersistedCatalogPublicationGuard<'a> {
    fn new(storage: &'a ChunkStorage) -> Self {
        Self {
            storage,
            visibility_guard: Some(storage.visibility_write_fence()),
            registry_catalog_sources: None,
        }
    }

    fn current_visibility_generation(&self) -> u64 {
        self.storage.visibility_state_generation()
    }

    pub(super) fn publish_transition(
        &mut self,
        mut transition: PersistedCatalogTransition,
    ) -> Result<PersistedCatalogRefreshApply> {
        let registry_catalog_sources = transition.registry_catalog_sources.take();
        let applied = self.storage.apply_persisted_catalog_transition_phase(
            transition,
            self.current_visibility_generation(),
        )?;
        if applied.is_applied() && registry_catalog_sources.is_some() {
            self.registry_catalog_sources = registry_catalog_sources;
        }
        Ok(applied)
    }

    pub(super) fn apply_planned_refresh(
        &mut self,
        planned: PlannedPersistedCatalogRefresh,
    ) -> Result<PersistedCatalogRefreshApply> {
        let transition = self
            .storage
            .plan_persisted_catalog_transition_phase(planned)?;
        self.publish_transition(transition)
    }

    pub(super) fn finish(mut self) -> Result<()> {
        self.visibility_guard = None;
        self.persist_registry_catalog()
    }

    fn persist_registry_catalog(&mut self) -> Result<()> {
        let Some(sources) = self.registry_catalog_sources.take() else {
            return Ok(());
        };
        #[cfg(test)]
        self.storage
            .invoke_registry_catalog_publication_persist_hook();
        self.storage
            .persist_series_registry_index_with_catalog_sources(&sources)
    }
}

impl Drop for PersistedCatalogPublicationGuard<'_> {
    fn drop(&mut self) {
        self.visibility_guard = None;
        if let Err(err) = self.persist_registry_catalog() {
            tracing::warn!(
                error = %err,
                "Failed to persist the registry catalog after a persisted catalog publication"
            );
        }
    }
}

impl ChunkStorage {
    pub(super) fn begin_persisted_catalog_publication(
        &self,
    ) -> PersistedCatalogPublicationGuard<'_> {
        PersistedCatalogPublicationGuard::new(self)
    }
}
