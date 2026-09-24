//! Concurrency utilities for tsink.

use parking_lot::{Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, instrument};

use crate::{Result, TsinkError};

/// A semaphore implementation for limiting concurrent operations.
#[derive(Clone)]
pub struct Semaphore {
    permits: Arc<Mutex<usize>>,
    max_permits: usize,
    condvar: Arc<Condvar>,
}

impl Semaphore {
    pub fn new(permits: usize) -> Self {
        let permits = permits.max(1);
        Self {
            permits: Arc::new(Mutex::new(permits)),
            max_permits: permits,
            condvar: Arc::new(Condvar::new()),
        }
    }

    #[instrument(skip(self))]
    pub fn acquire(&self) -> SemaphoreGuard<'_> {
        let mut permits = self.permits.lock();
        while *permits == 0 {
            self.condvar.wait(&mut permits);
        }

        *permits -= 1;
        debug!("Acquired semaphore permit, {} remaining", *permits);
        SemaphoreGuard { semaphore: self }
    }

    pub fn try_acquire(&self) -> Option<SemaphoreGuard<'_>> {
        let mut permits = self.permits.lock();
        if *permits == 0 {
            return None;
        }

        *permits -= 1;
        Some(SemaphoreGuard { semaphore: self })
    }

    pub fn try_acquire_for(&self, timeout: Duration) -> Result<SemaphoreGuard<'_>> {
        if timeout.is_zero() {
            return self.try_acquire().ok_or(TsinkError::WriteTimeout {
                timeout_ms: 0,
                workers: self.max_permits,
            });
        }

        let deadline = Instant::now() + timeout;
        let mut permits = self.permits.lock();
        loop {
            if *permits > 0 {
                *permits -= 1;
                return Ok(SemaphoreGuard { semaphore: self });
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(TsinkError::WriteTimeout {
                    timeout_ms: timeout.as_millis() as u64,
                    workers: self.max_permits,
                });
            }

            if self.condvar.wait_for(&mut permits, remaining).timed_out() && *permits == 0 {
                return Err(TsinkError::WriteTimeout {
                    timeout_ms: timeout.as_millis() as u64,
                    workers: self.max_permits,
                });
            }
        }
    }

    pub fn acquire_all(&self, timeout: Duration) -> Result<Vec<SemaphoreGuard<'_>>> {
        let timeout_err = || TsinkError::WriteTimeout {
            timeout_ms: timeout.as_millis() as u64,
            workers: self.max_permits,
        };

        if timeout.is_zero() {
            let mut permits = self.permits.lock();
            if *permits != self.max_permits {
                return Err(timeout_err());
            }
            *permits = 0;

            let mut guards = Vec::with_capacity(self.max_permits);
            for _ in 0..self.max_permits {
                guards.push(SemaphoreGuard { semaphore: self });
            }
            return Ok(guards);
        }

        let deadline = Instant::now() + timeout;
        let mut permits = self.permits.lock();
        loop {
            if *permits == self.max_permits {
                *permits = 0;
                let mut guards = Vec::with_capacity(self.max_permits);
                for _ in 0..self.max_permits {
                    guards.push(SemaphoreGuard { semaphore: self });
                }
                return Ok(guards);
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(timeout_err());
            }

            if self.condvar.wait_for(&mut permits, remaining).timed_out()
                && *permits != self.max_permits
            {
                return Err(timeout_err());
            }
        }
    }

    pub fn capacity(&self) -> usize {
        self.max_permits
    }

    fn release(&self) {
        let mut permits = self.permits.lock();
        debug_assert!(
            *permits < self.max_permits,
            "semaphore released more permits than its capacity"
        );
        if *permits < self.max_permits {
            *permits += 1;
        }
        debug!("Released semaphore permit, {} now available", *permits);

        self.condvar.notify_one();
    }

    pub fn available_permits(&self) -> usize {
        *self.permits.lock()
    }
}

/// Guard that automatically releases a semaphore permit when dropped.
pub struct SemaphoreGuard<'a> {
    semaphore: &'a Semaphore,
}

impl<'a> Drop for SemaphoreGuard<'a> {
    fn drop(&mut self) {
        self.semaphore.release();
    }
}

/// A read-write lock whose `read` never waits for a queued writer while any read
/// is held, so a thread can take a read again while it already holds one.
///
/// `parking_lot::RwLock::read` is task-fair: it queues behind a waiting writer even
/// when the calling thread already holds a read, so a thread that reads twice
/// while a writer queues in between deadlocks with that writer. Here `read` is
/// `read_recursive`. The price is reader preference: a waiting writer gets the lock
/// only once no read is held, so reads that keep overlapping keep it waiting.
pub(crate) struct RecursiveReaderPreferringRWLock<T>(RwLock<T>);

impl<T> RecursiveReaderPreferringRWLock<T> {
    pub(crate) fn new(value: T) -> Self {
        Self(RwLock::new(value))
    }

    pub(crate) fn read(&self) -> RwLockReadGuard<'_, T> {
        self.0.read_recursive()
    }

    pub(crate) fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.0.write()
    }

    #[cfg(test)]
    pub(crate) fn is_locked_exclusive(&self) -> bool {
        self.0.is_locked_exclusive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::{mpsc, Barrier};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_semaphore() {
        let sem = Semaphore::new(2);

        let _guard1 = sem.acquire();
        assert_eq!(sem.available_permits(), 1);

        let _guard2 = sem.acquire();
        assert_eq!(sem.available_permits(), 0);

        assert!(sem.try_acquire().is_none());

        drop(_guard1);
        assert_eq!(sem.available_permits(), 1);
    }

    #[test]
    fn semaphore_with_zero_permits_clamps_to_one() {
        let sem = Semaphore::new(0);
        assert_eq!(sem.capacity(), 1);
        assert_eq!(sem.available_permits(), 1);
    }

    #[test]
    fn try_acquire_for_returns_timeout_when_no_permits_are_available() {
        let sem = Semaphore::new(1);
        let _held = sem.acquire();

        let result = sem.try_acquire_for(Duration::from_millis(10));
        assert!(matches!(
            result,
            Err(TsinkError::WriteTimeout { workers: 1, .. })
        ));
    }

    #[test]
    fn try_acquire_for_zero_timeout_behaves_like_non_blocking_try() {
        let sem = Semaphore::new(1);
        let _held = sem.acquire();

        let result = sem.try_acquire_for(Duration::ZERO);
        assert!(matches!(
            result,
            Err(TsinkError::WriteTimeout {
                timeout_ms: 0,
                workers: 1
            })
        ));
    }

    #[test]
    fn try_acquire_for_succeeds_when_permit_is_released_before_deadline() {
        let sem = Arc::new(Semaphore::new(1));
        let held = sem.acquire();

        let waiter = {
            let sem = Arc::clone(&sem);
            thread::spawn(move || sem.try_acquire_for(Duration::from_millis(200)).is_ok())
        };

        thread::sleep(Duration::from_millis(25));
        drop(held);

        assert!(waiter.join().unwrap());
    }

    #[test]
    fn acquire_all_returns_all_permits_and_releases_on_drop() {
        let sem = Semaphore::new(3);
        let guards = sem.acquire_all(Duration::from_millis(20)).unwrap();

        assert_eq!(guards.len(), 3);
        assert_eq!(sem.available_permits(), 0);

        drop(guards);
        assert_eq!(sem.available_permits(), 3);
    }

    #[test]
    fn acquire_blocks_until_permit_is_released() {
        let sem = Arc::new(Semaphore::new(1));
        let held = sem.acquire();

        let (tx, rx) = mpsc::channel();
        let waiter = {
            let sem = Arc::clone(&sem);
            thread::spawn(move || {
                let _guard = sem.acquire();
                tx.send(()).unwrap();
            })
        };

        assert!(rx.recv_timeout(Duration::from_millis(20)).is_err());
        drop(held);
        assert!(rx.recv_timeout(Duration::from_millis(200)).is_ok());
        waiter.join().unwrap();
    }

    #[test]
    fn acquire_all_times_out_if_any_permit_remains_unavailable() {
        let sem = Semaphore::new(2);
        let _held = sem.acquire();

        let result = sem.acquire_all(Duration::from_millis(10));
        assert!(matches!(
            result,
            Err(TsinkError::WriteTimeout { workers: 2, .. })
        ));
    }

    #[test]
    fn concurrent_acquire_all_calls_do_not_split_permits() {
        let sem = Arc::new(Semaphore::new(2));
        let barrier = Arc::new(Barrier::new(3));

        let mut handles = Vec::new();
        for _ in 0..2 {
            let sem = Arc::clone(&sem);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                let guards = sem.acquire_all(Duration::from_millis(500)).unwrap();
                thread::sleep(Duration::from_millis(25));
                drop(guards);
            }));
        }

        barrier.wait();

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(sem.available_permits(), 2);
    }

    fn wait_for_writer<T>(lock: &RecursiveReaderPreferringRWLock<T>) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !lock.is_locked_exclusive() {
            assert!(
                std::time::Instant::now() < deadline,
                "the writer never queued"
            );
            thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn recursive_reader_preferring_rwlock_reads_again_while_a_writer_waits() {
        let lock = Arc::new(RecursiveReaderPreferringRWLock::new(()));
        let (outer_tx, outer_rx) = mpsc::channel();
        let (queued_tx, queued_rx) = mpsc::channel::<()>();
        let (done_tx, done_rx) = mpsc::channel();
        let reader = thread::spawn({
            let lock = Arc::clone(&lock);
            move || {
                let _outer = lock.read();
                outer_tx.send(()).unwrap();
                queued_rx.recv().unwrap();
                let _nested = lock.read();
                done_tx.send(()).unwrap();
            }
        });
        outer_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let writer = thread::spawn({
            let lock = Arc::clone(&lock);
            move || drop(lock.write())
        });
        wait_for_writer(&lock);
        queued_tx.send(()).unwrap();
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("a nested read waited for the queued writer");
        reader.join().unwrap();
        writer.join().unwrap();
    }

    #[test]
    fn recursive_reader_preferring_rwlock_lets_readers_pass_a_waiting_writer() {
        let lock = Arc::new(RecursiveReaderPreferringRWLock::new(0));
        let held = lock.read();
        let writer = thread::spawn({
            let lock = Arc::clone(&lock);
            move || *lock.write() += 1
        });
        wait_for_writer(&lock);
        let (read_tx, read_rx) = mpsc::channel();
        let reader = thread::spawn({
            let lock = Arc::clone(&lock);
            move || read_tx.send(*lock.read()).unwrap()
        });
        let passed = read_rx.recv_timeout(Duration::from_secs(10));
        drop(held);
        assert_eq!(passed, Ok(0), "a reader waited for the queued writer");
        reader.join().unwrap();
        writer.join().unwrap();
        assert_eq!(*lock.read(), 1);
    }
}
