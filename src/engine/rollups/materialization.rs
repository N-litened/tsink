use super::runtime::pending_materialized_through;
use super::*;

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

/// Upper bound on the rollup buckets a pass computes before it stages them. One staging write
/// of the rollup state covers every source in the batch, so a pass rewrites the state file once
/// per batch rather than once per source, while a large backfill holds only one batch of buckets
/// in memory (or one source's, when a single source has more).
const ROLLUP_STAGING_BATCH_POINTS: usize = 65_536;

impl RollupStateStoreContext<'_> {
    fn stage_pending_rollup_materializations<'p>(
        self,
        staged: impl IntoIterator<Item = (&'p str, &'p str, PendingRollupMaterialization)>,
    ) -> Result<()> {
        let checkpoints = self.checkpoints_snapshot();
        let generations = self.generations_snapshot();
        let mut pending_materializations = self.pending_materializations_snapshot();
        let pending_delete_invalidations = self.pending_delete_invalidations_snapshot();
        for (policy_id, source_key, pending) in staged {
            pending_materializations
                .entry(policy_id.to_string())
                .or_default()
                .insert(source_key.to_string(), pending);
        }
        self.persist_state_snapshot(
            &checkpoints,
            &generations,
            &pending_materializations,
            &pending_delete_invalidations,
        )?;
        *self.state.pending_materializations.write() = pending_materializations;
        Ok(())
    }

    fn mark_rollup_checkpoint_in_memory(
        self,
        policy_id: &str,
        source_key: &str,
        materialized_through: i64,
    ) {
        self.state
            .checkpoints
            .write()
            .entry(policy_id.to_string())
            .or_default()
            .insert(source_key.to_string(), materialized_through);
    }

    fn clear_pending_rollup_materialization_in_memory(self, policy_id: &str, source_key: &str) {
        let mut pending_materializations = self.state.pending_materializations.write();
        let remove_policy = pending_materializations
            .get_mut(policy_id)
            .is_some_and(|entries| {
                entries.remove(source_key);
                entries.is_empty()
            });
        if remove_policy {
            pending_materializations.remove(policy_id);
        }
    }

    fn policy_generation(self, policy_id: &str) -> u64 {
        self.state
            .generations
            .read()
            .get(policy_id)
            .copied()
            .unwrap_or(0)
    }

    fn set_rollup_policy_run_state(
        self,
        policy_id: &str,
        report: &PolicyRunReport,
        started_at_ms: u64,
        completed_at_ms: u64,
        duration_nanos: u64,
        error: Option<String>,
    ) {
        let mut stats = self.state.policy_stats.write();
        let state = stats.entry(policy_id.to_string()).or_default();
        state.matched_series = report.matched_series;
        state.materialized_series = report.materialized_series;
        state.materialized_through = report.materialized_through;
        state.last_run_started_at_ms = Some(started_at_ms);
        state.last_run_completed_at_ms = Some(completed_at_ms);
        state.last_run_duration_nanos = duration_nanos;
        state.last_error = error;
    }
}

impl RollupSourceReadContext<'_> {
    fn live_series_ids(
        self,
        candidate_series_ids: Vec<SeriesId>,
        prune_dead: bool,
    ) -> Result<Vec<SeriesId>> {
        self.ops.live_series_ids(candidate_series_ids, prune_dead)
    }

    fn query_tier_plan(self, start: i64, end: i64) -> TieredQueryPlan {
        self.ops.query_tier_plan(start, end)
    }

    fn collect_points_for_series_with_plan(
        self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
    ) -> Result<Vec<DataPoint>> {
        self.ops
            .collect_points_for_series_with_plan(series_id, start, end, plan)
    }

    pub(super) fn bounded_recency_reference_timestamp(self) -> Option<i64> {
        self.ops.bounded_recency_reference_timestamp()
    }

    fn rollup_sources_for_series_ids(
        self,
        policy: &RollupPolicy,
        series_ids: Vec<SeriesId>,
    ) -> Vec<RollupSourceSeries> {
        series_ids
            .into_iter()
            .filter_map(|series_id| {
                let series = self.registry.decode_series_key(series_id)?;
                if !policy_matches_source(policy, &series.metric, &series.labels) {
                    return None;
                }
                Some(RollupSourceSeries {
                    series_id,
                    source_key: source_series_key(&series.metric, &series.labels),
                    labels: series.labels,
                })
            })
            .collect()
    }

    fn matching_rollup_sources(self, policy: &RollupPolicy) -> Result<Vec<RollupSourceSeries>> {
        let candidate_series_ids = self.registry.series_ids_for_metric(&policy.metric);
        let live_series_ids = self.live_series_ids(candidate_series_ids, true)?;
        Ok(self.rollup_sources_for_series_ids(policy, live_series_ids))
    }

    pub(super) fn matching_rollup_sources_best_effort(
        self,
        policy: &RollupPolicy,
    ) -> Vec<RollupSourceSeries> {
        self.matching_rollup_sources(policy).unwrap_or_else(|_| {
            let candidate_series_ids = self.registry.series_ids_for_metric(&policy.metric);
            self.rollup_sources_for_series_ids(policy, candidate_series_ids)
        })
    }
}

impl RollupMaterializedWriteContext<'_> {
    fn insert_rows(self, rows: &[Row]) -> Result<WriteResult> {
        self.ops.insert_rows(rows)
    }
}

impl RollupInvalidationContext<'_> {
    fn rollup_policy_ids_needing_rebuild_for_rows(self, rows: &[Row]) -> BTreeSet<String> {
        if rows.is_empty() {
            return BTreeSet::new();
        }

        let policies = self.store.policies_snapshot();
        if policies.is_empty() {
            return BTreeSet::new();
        }
        let checkpoints = self.store.checkpoints_snapshot();
        let pending_materializations = self.store.pending_materializations_snapshot();
        let generations = self.store.generations_snapshot();

        let mut affected_policy_ids = BTreeSet::new();
        for row in rows {
            if is_internal_rollup_metric(row.metric()) {
                continue;
            }

            let source_key = source_series_key(row.metric(), row.labels());
            let timestamp = row.data_point().timestamp;
            for policy in &policies {
                if !policy_matches_source(policy, row.metric(), row.labels()) {
                    continue;
                }

                let Some(materialized_through) = checkpoints
                    .get(&policy.id)
                    .and_then(|entries| entries.get(&source_key))
                    .copied()
                    .or_else(|| {
                        pending_materialized_through(
                            &pending_materializations,
                            &generations,
                            &policy.id,
                            &source_key,
                        )
                    })
                else {
                    continue;
                };

                if timestamp < materialized_through {
                    affected_policy_ids.insert(policy.id.clone());
                }
            }
        }

        affected_policy_ids
    }
}

/// Buckets computed for one source, waiting to be staged and written.
struct PlannedRollupSource {
    policy: usize,
    source: RollupSourceSeries,
    rollup_metric: String,
    pending: PendingRollupMaterialization,
    points: Vec<DataPoint>,
}

struct PolicyRun {
    started: Instant,
    started_at_ms: u64,
    finished: Instant,
    report: PolicyRunReport,
    source_keys: Vec<String>,
    error: Option<String>,
}

/// One worker pass over every policy. Sources are planned policy by policy; their buckets are
/// staged together, so every source in a batch is covered by one persisted pending
/// materialization before any of its buckets is written, and written afterwards.
struct RollupPass<'p, 'c> {
    store: RollupStateStoreContext<'c>,
    source_reads: RollupSourceReadContext<'c>,
    materialized_writes: RollupMaterializedWriteContext<'c>,
    policies: &'p [RollupPolicy],
    runs: Vec<PolicyRun>,
    staged: Vec<PlannedRollupSource>,
    staged_points: usize,
    first_error: Option<TsinkError>,
}

impl<'p, 'c> RollupPass<'p, 'c> {
    fn new(
        store: RollupStateStoreContext<'c>,
        source_reads: RollupSourceReadContext<'c>,
        materialized_writes: RollupMaterializedWriteContext<'c>,
        policies: &'p [RollupPolicy],
    ) -> Self {
        let now = Instant::now();
        Self {
            store,
            source_reads,
            materialized_writes,
            policies,
            runs: policies
                .iter()
                .map(|_| PolicyRun {
                    started: now,
                    started_at_ms: 0,
                    finished: now,
                    report: PolicyRunReport::default(),
                    source_keys: Vec::new(),
                    error: None,
                })
                .collect(),
            staged: Vec::new(),
            staged_points: 0,
            first_error: None,
        }
    }

    fn fail(&mut self, policy: usize, err: TsinkError) {
        let run = &mut self.runs[policy];
        if run.error.is_none() {
            run.error = Some(err.to_string());
        }
        if self.first_error.is_none() {
            self.first_error = Some(err);
        }
    }

    fn run_policy(&mut self, policy: usize, max_observed: i64) {
        self.runs[policy].started = Instant::now();
        self.runs[policy].started_at_ms = now_unix_ms();
        if let Err(err) = self.plan_policy(policy, max_observed) {
            self.fail(policy, err);
        }
        self.runs[policy].finished = Instant::now();
    }

    fn plan_policy(&mut self, index: usize, max_observed: i64) -> Result<()> {
        let policies = self.policies;
        let policy = &policies[index];
        let sources = self.source_reads.matching_rollup_sources(policy)?;
        self.runs[index].report.matched_series = u64::try_from(sources.len()).unwrap_or(u64::MAX);
        self.runs[index].source_keys = sources
            .iter()
            .map(|source| source.source_key.clone())
            .collect();

        let Some(stable_end) = aligned_materialized_end(policy, max_observed) else {
            return Ok(());
        };

        let generation = self.store.policy_generation(&policy.id);
        let rollup_metric = rollup_metric_name(policy, generation);
        let existing_checkpoints = self
            .store
            .state
            .checkpoints
            .read()
            .get(&policy.id)
            .cloned()
            .unwrap_or_default();
        let existing_pending = self
            .store
            .state
            .pending_materializations
            .read()
            .get(&policy.id)
            .cloned()
            .unwrap_or_default();

        for source in sources {
            if self.runs[index].error.is_some() {
                break;
            }
            let checkpoint = existing_checkpoints
                .get(&source.source_key)
                .copied()
                .unwrap_or(i64::MIN);
            let target_end = existing_pending
                .get(&source.source_key)
                .filter(|pending| pending.generation == generation)
                .map(|pending| pending.materialized_through.max(stable_end))
                .unwrap_or(stable_end);
            if checkpoint >= target_end {
                continue;
            }

            let plan = self.source_reads.query_tier_plan(checkpoint, target_end);
            let raw_points = self.source_reads.collect_points_for_series_with_plan(
                source.series_id,
                checkpoint,
                target_end,
                plan,
            )?;
            let rollup_points = downsample_points_with_origin(
                &raw_points,
                policy.interval,
                policy.aggregation,
                policy.bucket_origin,
                checkpoint,
                target_end,
            )?;

            if rollup_points.is_empty() {
                self.store.mark_rollup_checkpoint_in_memory(
                    &policy.id,
                    &source.source_key,
                    target_end,
                );
                self.store
                    .clear_pending_rollup_materialization_in_memory(&policy.id, &source.source_key);
                self.runs[index].report.checkpoint_changed = true;
                continue;
            }

            self.staged_points = self.staged_points.saturating_add(rollup_points.len());
            self.staged.push(PlannedRollupSource {
                policy: index,
                source,
                rollup_metric: rollup_metric.clone(),
                pending: PendingRollupMaterialization {
                    checkpoint,
                    materialized_through: target_end,
                    generation,
                },
                points: rollup_points,
            });
            if self.staged_points >= ROLLUP_STAGING_BATCH_POINTS {
                self.write_staged();
            }
        }
        Ok(())
    }

    fn write_staged(&mut self) {
        let staged = std::mem::take(&mut self.staged);
        self.staged_points = 0;
        if staged.is_empty() {
            return;
        }

        let policies = self.policies;
        let staging = self
            .store
            .stage_pending_rollup_materializations(staged.iter().map(|item| {
                (
                    policies[item.policy].id.as_str(),
                    item.source.source_key.as_str(),
                    item.pending.clone(),
                )
            }));
        if let Err(err) = staging {
            let message = err.to_string();
            for item in &staged {
                let run = &mut self.runs[item.policy];
                if run.error.is_none() {
                    run.error = Some(message.clone());
                }
                run.finished = Instant::now();
            }
            if self.first_error.is_none() {
                self.first_error = Some(err);
            }
            return;
        }

        for item in staged {
            if self.runs[item.policy].error.is_some() {
                continue;
            }
            let policy_id = policies[item.policy].id.as_str();
            match self.write_planned(&item) {
                Ok(written) => {
                    let report = &mut self.runs[item.policy].report;
                    report.buckets_materialized =
                        report.buckets_materialized.saturating_add(written);
                    report.points_materialized = report.points_materialized.saturating_add(written);
                    report.checkpoint_changed = true;
                    self.store.mark_rollup_checkpoint_in_memory(
                        policy_id,
                        &item.source.source_key,
                        item.pending.materialized_through,
                    );
                    self.store.clear_pending_rollup_materialization_in_memory(
                        policy_id,
                        &item.source.source_key,
                    );
                }
                Err(err) => self.fail(item.policy, err),
            }
            self.runs[item.policy].finished = Instant::now();
        }
    }

    fn write_planned(&self, item: &PlannedRollupSource) -> Result<u64> {
        let checkpoint = item.pending.checkpoint;
        let target_end = item.pending.materialized_through;
        let existing_rollup_series_id = self
            .source_reads
            .registry
            .resolve_existing_series_id(item.rollup_metric.as_str(), &item.source.labels);
        let existing_bucket_timestamps = existing_rollup_series_id
            .map(|series_id| {
                self.source_reads.collect_points_for_series_with_plan(
                    series_id,
                    checkpoint,
                    target_end,
                    self.source_reads.query_tier_plan(checkpoint, target_end),
                )
            })
            .transpose()?
            .unwrap_or_default()
            .into_iter()
            .map(|point| point.timestamp)
            .collect::<BTreeSet<_>>();

        let rows = item
            .points
            .iter()
            .filter(|point| !existing_bucket_timestamps.contains(&point.timestamp))
            .map(|point| {
                Row::with_labels(
                    item.rollup_metric.clone(),
                    item.source.labels.clone(),
                    point.clone(),
                )
            })
            .collect::<Vec<_>>();
        if !rows.is_empty() {
            let _ = self.materialized_writes.insert_rows(&rows)?;
        }
        Ok(u64::try_from(rows.len()).unwrap_or(u64::MAX))
    }

    /// Records every policy's run state and returns whether any checkpoint moved.
    fn finish(self, observability: &RollupObservabilityCounters) -> (bool, Option<TsinkError>) {
        let checkpoints = self.store.checkpoints_snapshot();
        let mut checkpoints_dirty = false;
        for (policy, run) in self.policies.iter().zip(self.runs) {
            checkpoints_dirty |= run.report.checkpoint_changed;
            observability
                .buckets_materialized_total
                .fetch_add(run.report.buckets_materialized, Ordering::Relaxed);
            observability
                .points_materialized_total
                .fetch_add(run.report.points_materialized, Ordering::Relaxed);

            let mut report = run.report;
            let policy_checkpoints = checkpoints.get(&policy.id);
            let mut min_through = None::<i64>;
            let mut materialized_series = 0u64;
            for source_key in &run.source_keys {
                if let Some(materialized_through) = policy_checkpoints
                    .and_then(|entries| entries.get(source_key))
                    .copied()
                {
                    materialized_series = materialized_series.saturating_add(1);
                    min_through = Some(
                        min_through
                            .map(|current| current.min(materialized_through))
                            .unwrap_or(materialized_through),
                    );
                }
            }
            report.materialized_series = materialized_series;
            report.materialized_through = min_through;

            let duration_nanos = u64::try_from(run.finished.duration_since(run.started).as_nanos())
                .unwrap_or(u64::MAX);
            let completed_at_ms = run.started_at_ms.saturating_add(duration_nanos / 1_000_000);
            match run.error {
                None => self.store.set_rollup_policy_run_state(
                    &policy.id,
                    &report,
                    run.started_at_ms,
                    completed_at_ms,
                    duration_nanos,
                    None,
                ),
                Some(error) => self.store.set_rollup_policy_run_state(
                    &policy.id,
                    &PolicyRunReport::default(),
                    run.started_at_ms,
                    completed_at_ms,
                    duration_nanos,
                    Some(error),
                ),
            }
        }
        (checkpoints_dirty, self.first_error)
    }
}

// Caller must hold `rollup_run_lock`. This keeps policy-set replacement, persistence,
// and worker execution linearizable with each other so a worker only runs against a
// fully persisted policy snapshot.
fn run_rollup_pipeline_once_locked_impl(
    store: RollupStateStoreContext<'_>,
    source_reads: RollupSourceReadContext<'_>,
    materialized_writes: RollupMaterializedWriteContext<'_>,
    observability: &RollupObservabilityCounters,
) -> Result<()> {
    let started = Instant::now();
    observability
        .worker_runs_total
        .fetch_add(1, Ordering::Relaxed);

    let policies = store.policies_snapshot();
    if policies.is_empty() {
        let duration_nanos = elapsed_nanos_u64(started);
        observability
            .worker_success_total
            .fetch_add(1, Ordering::Relaxed);
        observability
            .last_run_duration_nanos
            .store(duration_nanos, Ordering::Relaxed);
        return Ok(());
    }

    let max_observed = source_reads
        .bounded_recency_reference_timestamp()
        .unwrap_or(i64::MIN);

    let mut pass = RollupPass::new(store, source_reads, materialized_writes, &policies);
    for (index, _policy) in policies.iter().enumerate() {
        observability
            .policy_runs_total
            .fetch_add(1, Ordering::Relaxed);
        #[cfg(test)]
        store.invoke_policy_start_hook(_policy);
        pass.run_policy(index, max_observed);
    }
    pass.write_staged();
    let (checkpoints_dirty, first_error) = pass.finish(observability);

    if checkpoints_dirty {
        let checkpoints = store.checkpoints_snapshot();
        let generations = store.generations_snapshot();
        let pending_materializations = store.pending_materializations_snapshot();
        let pending_delete_invalidations = store.pending_delete_invalidations_snapshot();
        store.persist_state_snapshot(
            &checkpoints,
            &generations,
            &pending_materializations,
            &pending_delete_invalidations,
        )?;
    }

    let duration_nanos = elapsed_nanos_u64(started);
    observability
        .last_run_duration_nanos
        .store(duration_nanos, Ordering::Relaxed);

    if let Some(err) = first_error {
        observability
            .worker_errors_total
            .fetch_add(1, Ordering::Relaxed);
        return Err(err);
    }

    observability
        .worker_success_total
        .fetch_add(1, Ordering::Relaxed);
    Ok(())
}

impl ChunkStorage {
    pub(in crate::engine) fn rollup_policy_ids_needing_rebuild_for_rows(
        &self,
        rows: &[Row],
    ) -> BTreeSet<String> {
        self.rollup_invalidation_context()
            .rollup_policy_ids_needing_rebuild_for_rows(rows)
    }

    pub(in crate::engine) fn run_rollup_pipeline_once_locked(&self) -> Result<()> {
        run_rollup_pipeline_once_locked_impl(
            self.rollup_state_store_context(),
            self.rollup_source_read_context(),
            self.rollup_materialized_write_context(),
            &self.observability.rollup,
        )
    }

    pub(in crate::engine) fn run_rollup_pipeline_once(&self) -> Result<()> {
        self.ensure_open()?;
        let _run_guard = self.rollup_run_coordination_context().run_lock.lock();
        self.run_rollup_pipeline_once_locked()
    }
}
