use super::*;

impl Compactor {
    pub(in crate::engine) fn compact_once_with_changes(&self) -> Result<CompactionOutcome> {
        finalize_pending_compaction_replacements(&self.data_path)?;
        let tombstones = load_tombstones(&self.data_path.join(TOMBSTONES_FILE_NAME))?;

        if let Some(outcome) = self.try_compact_level(
            CompactionLevel::L0,
            CompactionLevel::L1,
            self.l0_trigger,
            &tombstones,
        )? {
            return Ok(outcome);
        }

        if let Some(outcome) = self.try_compact_level(
            CompactionLevel::L1,
            CompactionLevel::L2,
            self.l1_trigger,
            &tombstones,
        )? {
            return Ok(outcome);
        }

        Ok(CompactionOutcome::default())
    }

    fn try_compact_level(
        &self,
        source: CompactionLevel,
        target: CompactionLevel,
        count_trigger: usize,
        tombstones: &TombstoneMap,
    ) -> Result<Option<CompactionOutcome>> {
        let source_level = level_to_u8(source);
        let target_level = level_to_u8(target);

        if count_level_segments_for_compaction(&self.data_path, source_level)? < 2 {
            return Ok(None);
        }

        let mut segments = load_segments_for_level_runtime_strict(&self.data_path, source_level)?;
        if segments.len() < 2 {
            return Ok(None);
        }

        segments.sort_by_key(|segment| segment.manifest.segment_id);

        let Some(window) =
            select_compaction_window(&segments, count_trigger, DEFAULT_SOURCE_WINDOW_SEGMENTS)
        else {
            return Ok(None);
        };
        if window.len() < 2 {
            return Ok(None);
        }

        let mut outcome = self.compact_segments(target_level, &window, tombstones)?;
        outcome.stats.compacted = true;
        outcome.stats.source_level = Some(source_level);
        outcome.stats.target_level = Some(target_level);
        Ok(Some(outcome))
    }
}

pub(super) fn count_level_segments_for_compaction(base: &Path, level: u8) -> Result<usize> {
    let expected_level_dir = format!("L{level}");
    let mut count = 0usize;

    for root in list_segment_dirs(base)? {
        if root
            .parent()
            .and_then(|parent| parent.file_name())
            .and_then(|name| name.to_str())
            != Some(expected_level_dir.as_str())
        {
            continue;
        }

        match read_segment_manifest(&root) {
            Ok(manifest) => {
                if manifest.level != level {
                    return Err(segment_validation_error(
                        &root,
                        SegmentValidationContext::Compaction,
                        &format!(
                            "segment directory level mismatch: stored under L{level}, manifest says L{}",
                            manifest.level
                        ),
                    ));
                }
                count = count.saturating_add(1);
            }
            Err(err) if is_not_found_error(&err) => continue,
            Err(TsinkError::DataCorruption(details)) => {
                return Err(segment_validation_error(
                    &root,
                    SegmentValidationContext::Compaction,
                    &details,
                ));
            }
            Err(err) => return Err(err),
        }
    }

    Ok(count)
}

/// Picks the source segments for one compaction pass, or `None` when the level
/// does not need compacting yet.
///
/// Segments whose chunks of the same series overlap in time are merged first,
/// regardless of the count trigger, so a series' persisted chunks go back to
/// being disjoint. Overlap is judged per series: two segments whose overall time
/// ranges intersect only because they hold different series (for example rollup
/// rows stamped at the start of an older bucket next to fresh raw samples) do not
/// count, since reads never have to reconcile their chunks.
pub(super) fn select_compaction_window(
    segments: &[LoadedSegment],
    count_trigger: usize,
    max_segments: usize,
) -> Option<Vec<&LoadedSegment>> {
    let max_segments = max_segments.max(2);
    if let Some(indexes) = overlapping_series_window_indexes(segments, max_segments) {
        return Some(
            indexes
                .into_iter()
                .filter_map(|index| segments.get(index))
                .collect(),
        );
    }

    if segments.len() < count_trigger {
        return None;
    }

    let window_len = count_trigger.max(2).min(max_segments).min(segments.len());
    Some(segments.iter().take(window_len).collect())
}

/// Groups segments that share a series whose chunks overlap in time and returns
/// the group holding the oldest such segment, limited to `max_segments` members in
/// storage order.
fn overlapping_series_window_indexes(
    segments: &[LoadedSegment],
    max_segments: usize,
) -> Option<Vec<usize>> {
    let mut chunk_ranges_by_series = HashMap::<SeriesId, Vec<(i64, i64, usize)>>::new();
    for (index, segment) in segments.iter().enumerate() {
        for (series_id, chunks) in &segment.chunks_by_series {
            chunk_ranges_by_series
                .entry(*series_id)
                .or_default()
                .extend(
                    chunks
                        .iter()
                        .filter(|chunk| chunk.header.point_count > 0)
                        .map(|chunk| (chunk.header.min_ts, chunk.header.max_ts, index)),
                );
        }
    }

    let mut groups = SegmentGroups::new(segments.len());
    for ranges in chunk_ranges_by_series.values_mut() {
        ranges.sort_unstable();
        let mut cluster: Option<(usize, i64)> = None;
        for &(min_ts, max_ts, index) in ranges.iter() {
            match cluster.as_mut() {
                Some((first_index, cluster_max_ts)) if min_ts <= *cluster_max_ts => {
                    groups.join(*first_index, index);
                    *cluster_max_ts = (*cluster_max_ts).max(max_ts);
                }
                _ => cluster = Some((index, max_ts)),
            }
        }
    }

    let oldest = (0..segments.len()).find(|&index| groups.size_of(index) >= 2)?;
    let root = groups.root(oldest);
    let mut indexes = (0..segments.len())
        .filter(|&index| groups.root(index) == root)
        .collect::<Vec<_>>();
    indexes.truncate(max_segments);
    Some(indexes)
}

struct SegmentGroups {
    parents: Vec<usize>,
    sizes: Vec<usize>,
}

impl SegmentGroups {
    fn new(len: usize) -> Self {
        Self {
            parents: (0..len).collect(),
            sizes: vec![1; len],
        }
    }

    fn root(&self, mut index: usize) -> usize {
        while self.parents[index] != index {
            index = self.parents[index];
        }
        index
    }

    fn size_of(&self, index: usize) -> usize {
        self.sizes[self.root(index)]
    }

    fn join(&mut self, left: usize, right: usize) {
        let (left, right) = (self.root(left), self.root(right));
        if left == right {
            return;
        }
        let (parent, child) = if self.sizes[left] >= self.sizes[right] {
            (left, right)
        } else {
            (right, left)
        };
        self.parents[child] = parent;
        self.sizes[parent] += self.sizes[child];
    }
}

pub(super) fn level_to_u8(level: CompactionLevel) -> u8 {
    match level {
        CompactionLevel::L0 => 0,
        CompactionLevel::L1 => 1,
        CompactionLevel::L2 => 2,
    }
}
