use std::collections::HashMap;

use tempfile::TempDir;

use super::*;
use crate::engine::chunk::{Chunk, ChunkHeader, ChunkPoint, ValueLane};
use crate::engine::encoder::Encoder;
use crate::engine::segment::{SegmentWriter, WalHighWatermark};
use crate::engine::series::SeriesValueFamily;
use crate::Value;

fn numeric_chunk(series_id: SeriesId, ts: i64) -> Chunk {
    let points = vec![ChunkPoint {
        ts,
        value: Value::F64(ts as f64),
    }];
    let encoded = Encoder::encode_chunk_points(&points, ValueLane::Numeric).unwrap();
    Chunk {
        header: ChunkHeader {
            series_id,
            lane: ValueLane::Numeric,
            value_family: Some(SeriesValueFamily::F64),
            point_count: 1,
            min_ts: ts,
            max_ts: ts,
            ts_codec: encoded.ts_codec,
            value_codec: encoded.value_codec,
        },
        points,
        encoded_payload: encoded.payload,
        wal_highwater: WalHighWatermark::default(),
    }
}

fn write_segment(
    lane: &Path,
    registry: &SeriesRegistry,
    segment_id: u64,
    series_ids: &[SeriesId],
) -> PersistedRegistryCatalogSource {
    let chunks = series_ids
        .iter()
        .map(|series_id| {
            (
                *series_id,
                vec![numeric_chunk(*series_id, segment_id as i64)],
            )
        })
        .collect::<HashMap<_, _>>();
    let writer = SegmentWriter::new(lane, 0, segment_id).unwrap();
    writer.write_segment(registry, &chunks).unwrap();
    PersistedRegistryCatalogSource {
        lane: SegmentLaneFamily::Numeric,
        root: writer.layout().root.clone(),
    }
}

fn series_id(registry: &SeriesRegistry, metric: &str, host: &str) -> SeriesId {
    registry
        .resolve_or_insert(metric, &[Label::new("host", host)])
        .unwrap()
        .series_id
}

fn make_unreadable(source: &PersistedRegistryCatalogSource) {
    for file in ["manifest.bin", "series.bin"] {
        std::fs::remove_file(source.root.join(file)).unwrap();
    }
}

#[test]
fn cached_catalog_matches_a_catalog_built_from_disk_as_segments_come_and_go() {
    let temp_dir = TempDir::new().unwrap();
    let lane = temp_dir.path().join("lane");
    let registry = SeriesRegistry::new();
    let a = series_id(&registry, "cpu", "a");
    let b = series_id(&registry, "cpu", "b");
    let c = series_id(&registry, "mem", "a");

    let s1 = write_segment(&lane, &registry, 1, &[a, b]);
    let s2 = write_segment(&lane, &registry, 2, &[b, c]);
    let s3 = write_segment(&lane, &registry, 3, &[c]);

    let mut cache = RegistryCatalogCache::default();
    let all = vec![s1.clone(), s2.clone(), s3.clone()];
    assert_eq!(cache.catalog(&all).unwrap(), build_catalog(&all).unwrap());

    let without_first = vec![s2.clone(), s3.clone()];
    assert_eq!(
        cache.catalog(&without_first).unwrap(),
        build_catalog(&without_first).unwrap()
    );
    assert_ne!(
        cache.catalog(&without_first).unwrap().series_fingerprint,
        build_catalog(&all).unwrap().series_fingerprint,
        "series only the removed segment held must leave the fingerprint"
    );

    let s4 = write_segment(&lane, &registry, 4, &[a]);
    let readded = vec![s4, s3, s2];
    assert_eq!(
        cache.catalog(&readded).unwrap(),
        build_catalog(&readded).unwrap()
    );
}

#[test]
fn segments_already_in_the_catalog_are_not_read_again() {
    let temp_dir = TempDir::new().unwrap();
    let lane = temp_dir.path().join("lane");
    let snapshot_path = temp_dir.path().join("series_index.bin");
    let registry = SeriesRegistry::new();
    let a = series_id(&registry, "cpu", "a");
    let b = series_id(&registry, "cpu", "b");

    let s1 = write_segment(&lane, &registry, 1, &[a]);
    let s2 = write_segment(&lane, &registry, 2, &[a, b]);
    let mut cache = RegistryCatalogCache::default();
    persist_registry_catalog(&snapshot_path, &[s1.clone(), s2.clone()], &mut cache).unwrap();
    let expected_first_entries = build_catalog(&[s1.clone(), s2.clone()]).unwrap().segments;

    make_unreadable(&s1);
    make_unreadable(&s2);
    let s3 = write_segment(&lane, &registry, 3, &[b]);
    persist_registry_catalog(&snapshot_path, &[s1, s2, s3.clone()], &mut cache).unwrap();

    let on_disk = read_catalog_file(&catalog_path(&snapshot_path))
        .unwrap()
        .expect("catalog written");
    assert_eq!(on_disk.segments.len(), 3);
    assert_eq!(on_disk.segments[..2], expected_first_entries[..]);
    assert_eq!(
        on_disk.segments[2],
        build_catalog(&[s3]).unwrap().segments[0]
    );
}

#[test]
fn an_unchanged_catalog_is_not_rewritten_but_a_missing_one_is() {
    let temp_dir = TempDir::new().unwrap();
    let lane = temp_dir.path().join("lane");
    let snapshot_path = temp_dir.path().join("series_index.bin");
    let path = catalog_path(&snapshot_path);
    let registry = SeriesRegistry::new();
    let a = series_id(&registry, "cpu", "a");
    let sources = vec![write_segment(&lane, &registry, 1, &[a])];

    let mut cache = RegistryCatalogCache::default();
    persist_registry_catalog(&snapshot_path, &sources, &mut cache).unwrap();
    std::fs::write(&path, b"left alone").unwrap();
    persist_registry_catalog(&snapshot_path, &sources, &mut cache).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"left alone");

    std::fs::remove_file(&path).unwrap();
    persist_registry_catalog(&snapshot_path, &sources, &mut cache).unwrap();
    assert_eq!(
        read_catalog_file(&path).unwrap(),
        Some(build_catalog(&sources).unwrap())
    );
}

#[test]
fn a_fresh_cache_adopts_a_matching_catalog_on_disk_without_reading_series_files() {
    let temp_dir = TempDir::new().unwrap();
    let lane = temp_dir.path().join("lane");
    let snapshot_path = temp_dir.path().join("series_index.bin");
    let path = catalog_path(&snapshot_path);
    let registry = SeriesRegistry::new();
    let a = series_id(&registry, "cpu", "a");
    let sources = vec![write_segment(&lane, &registry, 1, &[a])];

    persist_registry_catalog(&snapshot_path, &sources, &mut Default::default()).unwrap();
    let written = std::fs::read(&path).unwrap();
    let modified = std::fs::metadata(&path).unwrap().modified().unwrap();
    std::fs::remove_file(sources[0].root.join("series.bin")).unwrap();

    persist_registry_catalog(&snapshot_path, &sources, &mut Default::default()).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), written);
    assert_eq!(
        std::fs::metadata(&path).unwrap().modified().unwrap(),
        modified
    );
}

#[test]
fn a_fresh_cache_replaces_a_stale_or_corrupt_catalog_on_disk() {
    let temp_dir = TempDir::new().unwrap();
    let lane = temp_dir.path().join("lane");
    let snapshot_path = temp_dir.path().join("series_index.bin");
    let path = catalog_path(&snapshot_path);
    let registry = SeriesRegistry::new();
    let a = series_id(&registry, "cpu", "a");
    let b = series_id(&registry, "cpu", "b");
    let s1 = write_segment(&lane, &registry, 1, &[a]);
    let s2 = write_segment(&lane, &registry, 2, &[b]);
    let both = vec![s1.clone(), s2];

    persist_registry_catalog(&snapshot_path, &[s1], &mut Default::default()).unwrap();
    persist_registry_catalog(&snapshot_path, &both, &mut Default::default()).unwrap();
    assert_eq!(
        read_catalog_file(&path).unwrap(),
        Some(build_catalog(&both).unwrap())
    );

    std::fs::write(&path, b"{ not json").unwrap();
    persist_registry_catalog(&snapshot_path, &both, &mut Default::default()).unwrap();
    assert_eq!(
        read_catalog_file(&path).unwrap(),
        Some(build_catalog(&both).unwrap())
    );
}

#[test]
fn conflicting_series_metadata_is_rejected_without_corrupting_the_cache() {
    let temp_dir = TempDir::new().unwrap();
    let lane = temp_dir.path().join("lane");

    let registry = SeriesRegistry::new();
    let cpu = series_id(&registry, "cpu", "a");
    let s1 = write_segment(&lane, &registry, 1, &[cpu]);

    let other_key_same_id = SeriesRegistry::new();
    let mem = series_id(&other_key_same_id, "mem", "a");
    assert_eq!(mem, cpu);
    let s2 = write_segment(&lane, &other_key_same_id, 2, &[mem]);

    let same_key_other_id = SeriesRegistry::new();
    series_id(&same_key_other_id, "disk", "a");
    let cpu_again = series_id(&same_key_other_id, "cpu", "a");
    assert_ne!(cpu_again, cpu);
    let s3 = write_segment(&lane, &same_key_other_id, 3, &[cpu_again]);

    let mut cache = RegistryCatalogCache::default();
    let err = cache.catalog(&[s1.clone(), s2]).unwrap_err();
    assert!(
        matches!(&err, TsinkError::DataCorruption(message) if message.contains("conflicts across persisted segment metadata")),
        "{err}"
    );
    let err = cache.catalog(&[s1.clone(), s3]).unwrap_err();
    assert!(
        matches!(&err, TsinkError::DataCorruption(message) if message.contains("already bound")),
        "{err}"
    );
    assert_eq!(
        cache.catalog(std::slice::from_ref(&s1)).unwrap(),
        build_catalog(&[s1]).unwrap()
    );
}
