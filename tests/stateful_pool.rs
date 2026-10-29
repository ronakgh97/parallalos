use parallelos::pool::WorkerPool;
use parallelos::tasks::TaskCost;
use std::cell::Cell;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::{self, ThreadId};
use std::time::Duration;

struct LocalState {
    owner: ThreadId,
    runs: Rc<Cell<usize>>,
    dropped: mpsc::Sender<ThreadId>,
}

impl LocalState {
    fn new(dropped: mpsc::Sender<ThreadId>) -> Self {
        Self {
            owner: thread::current().id(),
            runs: Rc::new(Cell::new(0)),
            dropped,
        }
    }
}

impl Drop for LocalState {
    fn drop(&mut self) {
        assert_eq!(self.owner, thread::current().id());
        let _ = self.dropped.send(thread::current().id());
    }
}

#[test]
fn non_send_state_is_initialized_once_reused_and_dropped_on_its_worker() {
    let initialized = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&initialized);
    let (dropped_tx, dropped_rx) = mpsc::channel();
    let pool = WorkerPool::init_with_state(2, move || {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(LocalState::new(dropped_tx.clone()))
    })
    .unwrap();
    assert_eq!(initialized.load(Ordering::SeqCst), 2);

    let mut owners = Vec::new();
    for id in 0..2 {
        for expected in 1..=3 {
            let ((runs, owner), actual_id) = pool
                .submit_to_worker_with_state(
                    id,
                    |state| {
                        assert_eq!(state.owner, thread::current().id());
                        state.runs.set(state.runs.get() + 1);
                        (state.runs.get(), state.owner)
                    },
                    TaskCost::Normal,
                )
                .unwrap()
                .wait()
                .unwrap();
            assert_eq!((runs, actual_id), (expected, id));
            if expected == 1 {
                owners.push(owner);
            }
        }
    }
    assert_ne!(owners[0], owners[1]);
    drop(pool);
    let dropped: Vec<_> = dropped_rx.try_iter().collect();
    assert_eq!(dropped.len(), 2);
    assert!(owners.iter().all(|owner| dropped.contains(owner)));
    assert_eq!(initialized.load(Ordering::SeqCst), 2);
}

#[test]
fn non_send_state_pool_can_be_shared_between_submitters() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<WorkerPool<Rc<Cell<usize>>>>();

    let pool = Arc::new(WorkerPool::init_with_state(2, || Ok(Rc::new(Cell::new(0)))).unwrap());
    let submitter_pool = Arc::clone(&pool);
    let handle = thread::spawn(move || {
        submitter_pool.submit_with_state(|state| {
            state.set(state.get() + 1);
            state.get()
        })
    })
    .join()
    .unwrap()
    .unwrap();
    assert_eq!(handle.wait().unwrap().0, 1);
}

#[test]
fn different_workers_execute_stateful_tasks_concurrently() {
    let pool = WorkerPool::init_with_state(2, || Ok(thread::current().id())).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = crossbeam::channel::bounded(2);
    let mut handles = Vec::new();
    for id in 0..2 {
        let started_tx = started_tx.clone();
        let release_rx = release_rx.clone();
        handles.push(
            pool.submit_to_worker_with_state(
                id,
                move |state| {
                    started_tx.send(*state).unwrap();
                    release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                    *state
                },
                TaskCost::Normal,
            )
            .unwrap(),
        );
    }
    let first = started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let second = started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_ne!(first, second);
    release_tx.send(()).unwrap();
    release_tx.send(()).unwrap();
    for (id, handle) in handles.into_iter().enumerate() {
        let (owner, worker_id) = handle.wait().unwrap();
        assert_eq!(worker_id, id);
        assert!(owner == first || owner == second);
    }
}

#[test]
fn initializer_error_cleans_up_started_workers() {
    let (dropped_tx, dropped_rx) = mpsc::channel();
    let attempts = AtomicUsize::new(0);
    let result = WorkerPool::init_with_state(3, move || {
        if attempts.fetch_add(1, Ordering::SeqCst) == 1 {
            anyhow::bail!("session initialization failed");
        }
        Ok(LocalState::new(dropped_tx.clone()))
    });
    let error = result.err().expect("initialization should fail");
    assert!(format!("{error:#}").contains("session initialization failed"));
    let dropped: Vec<_> = dropped_rx.try_iter().collect();
    assert_eq!(dropped.len(), 1);
}

#[test]
fn initializer_panic_cleans_up_started_workers() {
    let (dropped_tx, dropped_rx) = mpsc::channel();
    let attempts = AtomicUsize::new(0);
    let result = WorkerPool::init_with_state(3, move || {
        assert_ne!(
            attempts.fetch_add(1, Ordering::SeqCst),
            1,
            "initializer panic"
        );
        Ok(LocalState::new(dropped_tx.clone()))
    });
    assert!(
        result
            .err()
            .unwrap()
            .to_string()
            .contains("initializer panicked")
    );
    let dropped: Vec<_> = dropped_rx.try_iter().collect();
    assert_eq!(dropped.len(), 1);
}

#[test]
fn task_panic_preserves_worker_and_mutated_state() {
    let pool = WorkerPool::init_with_state(2, || Ok(0usize)).unwrap();
    let task = pool
        .submit_to_worker_with_state(
            0,
            |state| {
                *state = 42;
                panic!("task panic");
            },
            TaskCost::Normal,
        )
        .unwrap();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| task.wait())).is_err());
    assert_eq!(
        pool.submit_to_worker_with_state(0, |state| *state, TaskCost::Normal)
            .unwrap()
            .wait()
            .unwrap(),
        (42, 0)
    );
}

#[test]
fn stateful_batch_keeps_lpt_order_and_legacy_tasks_work() {
    let pool = WorkerPool::init_with_state(2, || Ok(0usize)).unwrap();
    let tasks = (0..4).map(|id| {
        let cost = if id == 0 {
            TaskCost::Low
        } else {
            TaskCost::High
        };
        (
            move |state: &mut usize| {
                *state += 1;
                id
            },
            cost,
        )
    });
    let results: Vec<_> = pool
        .submit_batch_with_state(tasks)
        .unwrap()
        .into_iter()
        .map(|handle| handle.wait().unwrap().0)
        .collect();
    assert_eq!(results, [1, 2, 3, 0]);
    assert_eq!(pool.submit(|| 42).unwrap().wait().unwrap().0, 42);
    assert_eq!(
        pool.submit_with_state_and_cost(|state| *state, TaskCost::Low)
            .unwrap()
            .wait()
            .unwrap()
            .0,
        2
    );
}

#[test]
fn shutdown_drains_stateful_tasks_and_rejects_new_ones() {
    let (dropped_tx, dropped_rx) = mpsc::channel();
    let mut pool =
        WorkerPool::init_with_state(2, move || Ok(LocalState::new(dropped_tx.clone()))).unwrap();
    let handles: Vec<_> = (0..20)
        .map(|_| {
            pool.submit_with_state(|state| {
                state.runs.set(state.runs.get() + 1);
                state.runs.get()
            })
            .unwrap()
        })
        .collect();
    pool.shutdown().unwrap();
    for handle in handles {
        assert!(handle.wait().unwrap().0 > 0);
    }
    assert_eq!(dropped_rx.try_iter().count(), 2);
    assert!(pool.submit_with_state(|_| ()).is_err());
    assert!(
        pool.submit_to_worker_with_state(0, |_| (), TaskCost::Normal)
            .is_err()
    );
    pool.shutdown().unwrap();
}
