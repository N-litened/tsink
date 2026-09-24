use super::super::*;
use crate::engine::compactor::{CompactionRunStats, Compactor};

/// Merges small final-level segments of a store that no process has open.
///
/// Regular compaction never merges final-level (L2) segments, so a store fed a
/// steady trickle of writes accumulates many small ones until retention removes
/// them. This merges them per `window`-aligned span of their newest point, in
/// timestamp units, holding the data path lock for the whole run.
pub(in super::super) fn compact_final_level_offline(
    data_path: &Path,
    chunk_points: usize,
    window: i64,
) -> Result<CompactionRunStats> {
    if window <= 0 {
        return Err(TsinkError::InvalidConfiguration(
            "final-level compaction window must be positive".to_string(),
        ));
    }
    let _lock = process_lock::DataPathProcessLock::acquire(data_path)?;

    let lanes = [NUMERIC_LANE_ROOT, BLOB_LANE_ROOT].map(|lane| data_path.join(lane));
    let mut scanned = Vec::with_capacity(lanes.len());
    let mut max_segment_id = None::<u64>;
    for lane in &lanes {
        let (segments, lane_max) =
            Compactor::new(lane, chunk_points).scan_final_level_segments()?;
        max_segment_id = max_segment_id.max(lane_max);
        scanned.push(segments);
    }
    let next_segment_id = Arc::new(AtomicU64::new(
        max_segment_id.map_or(1, |max| max.saturating_add(1)),
    ));

    let mut total = CompactionRunStats::default();
    for (lane, mut segments) in lanes.iter().zip(scanned) {
        let compactor = Compactor::new_with_segment_id_allocator(
            lane,
            chunk_points,
            Arc::clone(&next_segment_id),
        );
        loop {
            let stats = compactor.compact_final_level_once(&mut segments, window)?;
            if !stats.compacted {
                break;
            }
            total.compacted = true;
            total.source_level = stats.source_level;
            total.target_level = stats.target_level;
            total.source_segments += stats.source_segments;
            total.output_segments += stats.output_segments;
            total.source_chunks += stats.source_chunks;
            total.output_chunks += stats.output_chunks;
            total.source_points += stats.source_points;
            total.output_points += stats.output_points;
        }
    }
    Ok(total)
}
