//! Queries that hold the visibility read fence across several reads must not
//! deadlock against a persisted-catalog or tombstone publication that queues for
//! the write fence in between, and no read may wait for the registry catalog
//! sidecar that a publication writes once the new state is visible.

use super::*;
use crate::{Aggregation, QueryOptions};
use parking_lot::Mutex;
use std::sync::mpsc;
use std::time::Instant;

const DEADLOCK_TIMEOUT: Duration = Duration::from_secs(10);
const READ_TIMEOUT: Duration = Duration::from_secs(2);

fn cpu_rows(labels: &[Label]) -> Vec<Row> {
    [
        (0, 1.0),
        (1_000, 3.0),
        (2_000, 5.0),
        (3_000, 7.0),
        (4_000, 9.0),
    ]
    .into_iter()
    .map(|(ts, value)| Row::with_labels("cpu_usage", labels.to_vec(), DataPoint::new(ts, value)))
    .collect()
}

fn wait_for_pending_writer(storage: &ChunkStorage) {
    let deadline = Instant::now() + DEADLOCK_TIMEOUT;
    while !storage.visibility_writer_pending() {
        assert!(
            Instant::now() < deadline,
            "the publication never queued for the write fence"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn assert_downsampled_select_survives_a_queued_publication(
    storage: Arc<ChunkStorage>,
    labels: Vec<Label>,
) {
    let (fenced_tx, fenced_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let fenced_tx = Mutex::new(fenced_tx);
    let release_rx = Mutex::new(release_rx);
    storage.set_query_visibility_fenced_hook(move || {
        let _ = fenced_tx.lock().send(());
        let _ = release_rx.lock().recv_timeout(DEADLOCK_TIMEOUT);
    });

    let (done_tx, done_rx) = mpsc::channel();
    let query = std::thread::spawn({
        let storage = Arc::clone(&storage);
        move || {
            let options = QueryOptions::new(0, 6_000)
                .with_labels(labels)
                .with_downsample(2_000, Aggregation::Avg);
            let _ = done_tx.send(storage.select_with_options("cpu_usage", options));
        }
    });
    fenced_rx
        .recv_timeout(DEADLOCK_TIMEOUT)
        .expect("the downsampled select never took the visibility fence");

    let publication = std::thread::spawn({
        let storage = Arc::clone(&storage);
        move || {
            let _publication = storage.visibility_write_fence();
        }
    });
    wait_for_pending_writer(&storage);
    release_tx.send(()).unwrap();

    let points = done_rx
        .recv_timeout(DEADLOCK_TIMEOUT)
        .expect("the downsampled select deadlocked behind the queued publication")
        .unwrap();
    assert_eq!(
        points,
        vec![
            DataPoint::new(0, 2.0),
            DataPoint::new(2_000, 6.0),
            DataPoint::new(4_000, 9.0),
        ]
    );
    query.join().unwrap();
    publication.join().unwrap();
    storage.clear_query_visibility_fenced_hook();
}

#[test]
fn downsampled_select_survives_a_publication_queued_behind_its_fence() {
    let temp_dir = TempDir::new().unwrap();
    let storage = persistent_rollup_storage(temp_dir.path());
    let labels = vec![Label::new("host", "a")];
    storage.insert_rows(&cpu_rows(&labels)).unwrap();

    assert_downsampled_select_survives_a_queued_publication(Arc::clone(&storage), labels);
    storage.close().unwrap();
}

#[test]
fn rollup_backed_select_survives_a_publication_queued_behind_its_fence() {
    let temp_dir = TempDir::new().unwrap();
    let storage = persistent_rollup_storage(temp_dir.path());
    let labels = vec![Label::new("host", "a")];
    storage.insert_rows(&cpu_rows(&labels)).unwrap();
    storage
        .apply_rollup_policies(vec![crate::storage::RollupPolicy {
            id: "cpu_2s_avg".to_string(),
            metric: "cpu_usage".to_string(),
            match_labels: Vec::new(),
            interval: 2_000,
            aggregation: Aggregation::Avg,
            bucket_origin: 0,
        }])
        .unwrap();
    let plans_before = storage
        .observability_snapshot()
        .query
        .rollup_query_plans_total;

    assert_downsampled_select_survives_a_queued_publication(Arc::clone(&storage), labels);
    assert_eq!(
        storage
            .observability_snapshot()
            .query
            .rollup_query_plans_total,
        plans_before + 1,
        "the select should have been served from the rollup"
    );
    storage.close().unwrap();
}

fn assert_reads_proceed_while_a_publication_persists_the_registry_catalog<F>(
    storage: Arc<ChunkStorage>,
    labels: Vec<Label>,
    expected_points: usize,
    publish: F,
) where
    F: FnOnce(&ChunkStorage) -> crate::Result<()> + Send + 'static,
{
    let (persisting_tx, persisting_rx) = mpsc::channel::<bool>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let persisting_tx = Mutex::new(persisting_tx);
    let release_rx = Mutex::new(release_rx);
    let hooked_storage = Arc::downgrade(&storage);
    storage.set_registry_catalog_publication_persist_hook(move || {
        let fenced = hooked_storage
            .upgrade()
            .is_some_and(|storage| storage.visibility_writer_pending());
        let _ = persisting_tx.lock().send(fenced);
        let _ = release_rx.lock().recv_timeout(DEADLOCK_TIMEOUT);
    });

    let publication = std::thread::spawn({
        let storage = Arc::clone(&storage);
        move || publish(&storage)
    });
    let fenced = persisting_rx
        .recv_timeout(DEADLOCK_TIMEOUT)
        .expect("the publication never persisted the registry catalog");

    let (read_tx, read_rx) = mpsc::channel();
    let read = std::thread::spawn({
        let storage = Arc::clone(&storage);
        move || {
            let _ = read_tx.send(storage.select("cpu_usage", &labels, 0, 10_000));
        }
    });
    let points = read_rx.recv_timeout(READ_TIMEOUT);
    release_tx.send(()).unwrap();
    publication.join().unwrap().unwrap();
    read.join().unwrap();
    storage.clear_registry_catalog_publication_persist_hook();

    assert!(
        !fenced,
        "the registry catalog was persisted while holding the visibility write fence"
    );
    assert_eq!(
        points
            .expect("a read waited for the registry catalog to be persisted")
            .unwrap()
            .len(),
        expected_points
    );
}

#[test]
fn reads_do_not_wait_for_the_registry_catalog_of_post_flush_maintenance() {
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(persistent_numeric_storage(
        temp_dir.path(),
        TimestampPrecision::Milliseconds,
        8,
    ));
    let labels = vec![Label::new("host", "a")];
    storage.insert_rows(&cpu_rows(&labels)).unwrap();
    storage.flush_pipeline_once().unwrap();

    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let catalog_path = super::super::registry_catalog::catalog_path(&checkpoint_path);
    std::fs::remove_file(&catalog_path).unwrap();

    assert_reads_proceed_while_a_publication_persists_the_registry_catalog(
        Arc::clone(&storage),
        labels,
        5,
        |storage| storage.sweep_expired_persisted_segments().map(|_| ()),
    );

    assert!(
        super::super::registry_catalog::validate_registry_catalog(
            &checkpoint_path,
            &storage
                .persisted
                .persisted_index
                .read()
                .segments_by_root
                .iter()
                .map(|(root, segment)| {
                    super::super::registry_catalog::PersistedRegistryCatalogSource {
                        lane: segment.lane,
                        root: root.clone(),
                    }
                },)
                .collect::<Vec<_>>(),
        )
        .unwrap()
        .is_some(),
        "the publication should still persist a registry catalog matching the published segments"
    );
    storage.close().unwrap();
}

#[test]
fn reads_do_not_wait_for_the_registry_catalog_of_a_compaction_refresh() {
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(persistent_numeric_storage(
        temp_dir.path(),
        TimestampPrecision::Milliseconds,
        8,
    ));
    let labels = vec![Label::new("host", "a")];
    storage.insert_rows(&cpu_rows(&labels)).unwrap();
    storage.flush_pipeline_once().unwrap();
    storage
        .insert_rows(&[
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(500, 2.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(1_500, 4.0)),
        ])
        .unwrap();
    storage.flush_pipeline_once().unwrap();

    let changes = ChunkStorage::compact_compactors_with_changes(
        storage.persisted.numeric_compactor.as_ref(),
        None,
        None,
    )
    .unwrap();
    assert!(
        !changes.is_empty(),
        "segments overlapping within a series should be compacted"
    );
    storage
        .persisted
        .pending_persisted_segment_diff
        .lock()
        .merge(changes);
    storage
        .persisted
        .persisted_index_dirty
        .store(true, std::sync::atomic::Ordering::SeqCst);

    assert_reads_proceed_while_a_publication_persists_the_registry_catalog(
        Arc::clone(&storage),
        labels,
        7,
        |storage| storage.sync_persisted_segments_from_disk_if_dirty(),
    );

    assert!(
        !storage
            .persisted
            .persisted_index_dirty
            .load(std::sync::atomic::Ordering::SeqCst),
        "the compaction refresh should have been applied"
    );
    storage.close().unwrap();
}
