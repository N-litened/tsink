use super::*;

#[test]
fn offline_final_level_compaction_merges_small_segments_and_keeps_the_store_readable() {
    let temp_dir = TempDir::new().unwrap();
    let lane = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &labels)
        .unwrap()
        .series_id;

    let mut expected = Vec::new();
    for segment_id in 1..=6u64 {
        let ts = segment_id as i64 * 10;
        let mut chunks = HashMap::new();
        chunks.insert(
            series_id,
            vec![make_persisted_numeric_chunk(series_id, &[(ts, ts as f64)])],
        );
        SegmentWriter::new(&lane, 2, segment_id)
            .unwrap()
            .write_segment(&registry, &chunks)
            .unwrap();
        expected.push(DataPoint::new(ts, ts as f64));
    }

    let builder = || {
        StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_retention_enforced(false)
            .with_chunk_points(8)
    };
    let stats = builder()
        .compact_final_level(Duration::from_secs(3_600))
        .unwrap();
    assert!(stats.compacted);
    assert_eq!(stats.source_segments, 6);
    assert_eq!(stats.output_segments, 1);
    assert_eq!(stats.output_points, 6);
    assert_eq!(load_segments_for_level(&lane, 2).unwrap().len(), 1);

    let storage = builder().build().unwrap();
    assert_eq!(storage.select("cpu", &labels, 0, 100).unwrap(), expected);

    let err = builder()
        .compact_final_level(Duration::from_secs(3_600))
        .unwrap_err();
    assert!(err.to_string().contains("already locked"), "{err}");
    storage.close().unwrap();
}

#[test]
fn offline_final_level_compaction_rejects_stores_it_cannot_handle() {
    let temp_dir = TempDir::new().unwrap();
    assert!(StorageBuilder::new()
        .compact_final_level(Duration::from_secs(3_600))
        .is_err());
    assert!(StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .compact_final_level(Duration::ZERO)
        .is_err());
    assert!(StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_object_store_path(temp_dir.path().join("objects"))
        .compact_final_level(Duration::from_secs(3_600))
        .is_err());
}
