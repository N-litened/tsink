use super::*;

/// A final-level segment as seen by [`Compactor::compact_final_level_once`].
#[derive(Debug, Clone)]
pub(in crate::engine) struct FinalLevelSegment {
    pub(in crate::engine) root: PathBuf,
    pub(in crate::engine) manifest: crate::engine::segment::SegmentManifest,
}

impl Compactor {
    /// Lists the final-level segments and the highest segment id at any level,
    /// reading each segment manifest once.
    pub(in crate::engine) fn scan_final_level_segments(
        &self,
    ) -> Result<(Vec<FinalLevelSegment>, Option<u64>)> {
        finalize_pending_compaction_replacements(&self.data_path)?;
        let final_level = super::planning::level_to_u8(CompactionLevel::L2);
        let mut segments = Vec::new();
        let mut max_segment_id = None::<u64>;
        for root in list_segment_dirs(&self.data_path)? {
            let manifest = read_segment_manifest(&root)?;
            max_segment_id = Some(
                max_segment_id.map_or(manifest.segment_id, |max| max.max(manifest.segment_id)),
            );
            if manifest.level == final_level {
                segments.push(FinalLevelSegment { root, manifest });
            }
        }
        Ok((segments, max_segment_id))
    }

    /// Merges one group of small final-level (L2) segments, which regular
    /// compaction never touches, and updates `segments` to match.
    ///
    /// Segments are grouped by the `window`-aligned span (in timestamp units) that
    /// holds their newest point, so merged data still expires together. A segment
    /// is small below half of the output segment point budget; each group takes the
    /// small segments of the earliest window holding at least two of them, oldest
    /// data first, until it reaches the budget or the source window size. A group
    /// of `n` sources therefore writes at most `n - 1` segments, and calling this
    /// until it reports no compaction terminates.
    pub(in crate::engine) fn compact_final_level_once(
        &self,
        segments: &mut Vec<FinalLevelSegment>,
        window: i64,
    ) -> Result<CompactionRunStats> {
        let Some(group) = select_final_level_merge(
            segments,
            window,
            self.output_segment_point_budget(),
            DEFAULT_SOURCE_WINDOW_SEGMENTS,
        ) else {
            return Ok(CompactionRunStats::default());
        };

        let tombstones = load_tombstones(&self.data_path.join(TOMBSTONES_FILE_NAME))?;
        let loaded = group
            .iter()
            .map(|&index| crate::engine::segment::load_segment(&segments[index].root))
            .collect::<Result<Vec<_>>>()?;
        let sources = loaded.iter().collect::<Vec<_>>();
        let final_level = super::planning::level_to_u8(CompactionLevel::L2);
        let outcome = self.compact_segments(final_level, &sources, &tombstones)?;

        let outputs = outcome
            .output_roots
            .iter()
            .map(|root| {
                Ok(FinalLevelSegment {
                    root: root.clone(),
                    manifest: read_segment_manifest(root)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut index = 0usize;
        segments.retain(|_| {
            let keep = !group.contains(&index);
            index += 1;
            keep
        });
        segments.extend(outputs);

        let mut stats = outcome.stats;
        stats.source_level = Some(final_level);
        stats.target_level = Some(final_level);
        Ok(stats)
    }

    fn output_segment_point_budget(&self) -> usize {
        self.point_cap
            .saturating_mul(DEFAULT_OUTPUT_SEGMENT_CHUNK_MULTIPLIER)
            .max(self.point_cap)
    }
}

pub(super) fn select_final_level_merge(
    segments: &[FinalLevelSegment],
    window: i64,
    point_budget: usize,
    max_sources: usize,
) -> Option<Vec<usize>> {
    let window = window.max(1);
    let small_limit = point_budget / 2;
    let mut small_by_window = BTreeMap::<i64, Vec<usize>>::new();
    for (index, segment) in segments.iter().enumerate() {
        let (Some(_), Some(max_ts)) = (segment.manifest.min_ts, segment.manifest.max_ts) else {
            continue;
        };
        if segment.manifest.point_count >= small_limit {
            continue;
        }
        small_by_window
            .entry(max_ts.div_euclid(window))
            .or_default()
            .push(index);
    }

    for mut indexes in small_by_window.into_values() {
        if indexes.len() < 2 {
            continue;
        }
        indexes.sort_by_key(|&index| {
            (
                segments[index].manifest.min_ts,
                segments[index].manifest.segment_id,
            )
        });
        let mut group = Vec::new();
        let mut points = 0usize;
        for index in indexes {
            group.push(index);
            points = points.saturating_add(segments[index].manifest.point_count);
            if points >= point_budget || group.len() >= max_sources.max(2) {
                break;
            }
        }
        return Some(group);
    }
    None
}
