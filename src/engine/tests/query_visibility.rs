//! Queries that hold the visibility read fence across several reads must not
//! deadlock against a persisted-catalog or tombstone publication that queues for
//! the write fence in between.

use super::*;
use crate::{Aggregation, QueryOptions};
use parking_lot::Mutex;
use std::sync::mpsc;
use std::time::Instant;

const DEADLOCK_TIMEOUT: Duration = Duration::from_secs(10);

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
