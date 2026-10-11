use parallelos::pool::WorkerPool;
use parallelos::tasks::{TaskCost, TaskHandle};
use sha2::Digest;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Poll `cond` until it holds, or fail after a 5s deadline.
/// A real regression trips the deadline; a slow machine doesn't flake it.
fn wait_until(mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    false
}

fn assert_balanced(p: &WorkerPool) {
    assert!(
        wait_until(|| p.stats().iter().all(|w| w.tasks_cost == 0)),
        "task_load never balanced: {:?}",
        p.stats()
    );
}

// --- load accounting

#[test]
fn loads_balance_after_all_tasks_complete() {
    let p = WorkerPool::init_with(4).unwrap();
    let handles: Vec<TaskHandle<u64>> = (0..64u64)
        .map(|i| {
            let c = match i % 5 {
                0 => TaskCost::Low,
                1 => TaskCost::Normal,
                2 => TaskCost::Moderate,
                3 => TaskCost::High,
                _ => TaskCost::VeryHigh,
            };
            p.submit_with(move || i * i, c).unwrap()
        })
        .collect();

    for h in handles {
        h.wait().unwrap();
    }

    assert!(
        p.stats().iter().all(|w| w.tasks_cost == 0),
        "unbalanced loads: {:?}",
        p.stats()
    );
}

#[test]
fn fire_and_forget_tasks_still_balance() {
    let p = WorkerPool::init_with(4).unwrap();
    // drop every handle immediately; nobody waits.
    for i in 0..64u64 {
        let c = match i % 5 {
            0 => TaskCost::Low,
            1 => TaskCost::Normal,
            2 => TaskCost::Moderate,
            3 => TaskCost::High,
            _ => TaskCost::VeryHigh,
        };
        let _ = p.submit_with(move || i, c);
    }
    assert_balanced(&p);
}

#[test]
fn low_cost_tasks_count_toward_load() {
    let p = WorkerPool::init_with(2).unwrap();
    p.submit_with(|| 1u64, TaskCost::Low)
        .unwrap()
        .wait()
        .unwrap();
    p.submit_with(|| 2u64, TaskCost::Low)
        .unwrap()
        .wait()
        .unwrap();
    assert_balanced(&p);
}

// --- results and panics

#[test]
fn every_task_returns_its_value() {
    let p = WorkerPool::init_with(4).unwrap();
    let sum: u64 = (0..64u64)
        .map(|i| p.submit(move || i * i).unwrap())
        .map(|h| h.wait().unwrap().0)
        .sum();

    assert_eq!(sum, (0..64u64).map(|i| i * i).sum());
}

#[test]
fn a_panicking_task_does_not_kill_its_worker() {
    let p = WorkerPool::init_with(2).unwrap();

    let h = p.submit(|| -> () { panic!("boom") }).unwrap();
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = h.wait();
    }));
    assert!(caught.is_err(), "panic should cross the thread boundary");

    assert_eq!(p.submit(|| 7u64).unwrap().wait().unwrap().0, 7);
    assert_balanced(&p);
}

// --- shutdown and Drop

#[test]
fn submit_after_shutdown_is_refused() {
    let mut p = WorkerPool::init_with(2).unwrap();
    p.shutdown().unwrap();
    assert!(p.submit(|| 1).is_err());
}

#[test]
fn drop_without_shutdown_still_completes_tasks() {
    let p = WorkerPool::init_with(2).unwrap();
    let h = p.submit(|| 42u64).unwrap();
    drop(p); // Drop runs shutdown(), which drains the queue and runs the task.
    assert_eq!(h.wait().unwrap().0, 42);
}

#[test]
fn shutdown_drains_queued_tasks_without_waiting() {
    let mut p = WorkerPool::init_with(4).unwrap();
    for _ in 0..200 {
        let _ = p.submit(|| ());
    }
    // A drain failure shows up as a hang or an Err here.
    p.shutdown().unwrap();
}

// --- contention

#[test]
fn concurrent_submit_balances() {
    let p = Arc::new(WorkerPool::init_with(4).unwrap());

    let expected: u64 = (0..4u64)
        .flat_map(|t| (0..64u64).map(move |i| t * 1000 + i))
        .sum();

    let submitters: Vec<_> = (0..4u64)
        .map(|t| {
            let p = Arc::clone(&p);
            std::thread::spawn(move || {
                (0..64u64)
                    .map(|i| p.submit(move || t * 1000 + i).unwrap())
                    .collect::<Vec<TaskHandle<u64>>>()
            })
        })
        .collect();

    let handles: Vec<TaskHandle<u64>> = submitters
        .into_iter()
        .flat_map(|s| s.join().unwrap())
        .collect();

    let actual: u64 = handles.into_iter().map(|h| h.wait().unwrap().0).sum();
    assert_eq!(actual, expected);

    assert!(
        wait_until(|| p.stats().iter().all(|w| w.tasks_cost == 0)),
        "task_load never balanced: {:?}",
        p.stats()
    );
}

#[test]
fn batch_spreads_work_across_all_workers() {
    const WORKERS: usize = 8;
    const TASKS: usize = 512;

    let p = WorkerPool::init_with(WORKERS).unwrap();

    // alternate Low/High so the LPT sort has real ordering work to do
    let tasks = (0..TASKS).map(|i| {
        let cost = if i % 2 == 0 {
            TaskCost::Low
        } else {
            TaskCost::High
        };
        (move || black_box(i), cost)
    });

    let handles = p.submit_batch(tasks).unwrap();
    assert_eq!(handles.len(), TASKS, "batch dropped task handles");

    for handle in handles {
        let _ = handle.wait().unwrap();
    }

    let executed: Vec<u64> = p.stats().iter().map(|w| w.tasks_executed).collect();
    assert!(
        executed.iter().all(|&n| n == TASKS as u64 / WORKERS as u64),
        "LPT should spread the batch evenly, got {executed:?}"
    );

    assert_balanced(&p);
}

#[test]
fn batch_with_uniform_costs_spreads_evenly() {
    const WORKERS: usize = 8;
    const TASKS: usize = 256;

    let p = WorkerPool::init_with(WORKERS).unwrap();

    let tasks = (0..TASKS).map(|i| (move || black_box(i), TaskCost::Normal));
    let handles = p.submit_batch(tasks).unwrap();
    assert_eq!(handles.len(), TASKS, "batch dropped task handles");

    for handle in handles {
        let _ = handle.wait().unwrap();
    }

    let executed: Vec<u64> = p.stats().iter().map(|w| w.tasks_executed).collect();
    assert!(
        executed.iter().all(|&n| n > 0),
        "some worker got no work at all: {executed:?}"
    );
    assert!(
        executed.iter().all(|&n| n == TASKS as u64 / WORKERS as u64),
        "uniform-cost batch should round-robin evenly, got {executed:?}"
    );

    assert_balanced(&p);
}

#[test]
fn batch_refused_after_shutdown() {
    let mut p = WorkerPool::init_with(4).unwrap();
    p.shutdown().unwrap();

    let result = p.submit_batch((0..8).map(|i| (move || black_box(i), TaskCost::Normal)));

    assert!(result.is_err(), "batch should refuse on a shut down pool");
}

#[test]
fn wait_all_tasks_waits_for_queued_and_running() {
    let p = WorkerPool::init_with(4).unwrap();

    // more tasks than workers, and slow enough that some are still running
    // when the last one is dequeued
    for _ in 0..16 {
        let _ = p.submit_with(
            || std::thread::sleep(Duration::from_millis(20)),
            TaskCost::Low,
        );
    }

    p.wait_all().unwrap();

    let stats = p.stats();
    assert!(
        stats.iter().all(|w| w.tasks_cost == 0),
        "pool not drained: {stats:?}"
    );
    assert_eq!(
        stats.iter().map(|w| w.tasks_executed).sum::<u64>(),
        16,
        "wait_all_tasks returned before every task ran"
    );
}

#[test]
fn wait_all_tasks_returns_immediately_on_idle_pool() {
    let p = WorkerPool::init_with(4).unwrap();
    p.wait_all().unwrap();
}

// Reference perf point for later refactors, not a correctness test, RUN IN RELEASE MODE
#[test]
fn perf_regress_reference_single() {
    let p = WorkerPool::init_with(8).unwrap();

    const TASK_COUNT: usize = 178956; // MAGIC NUMBER, DO NOT CHANGE!!!

    let start = Instant::now();
    for _ in 0..TASK_COUNT {
        if fastrand::bool() {
            p.submit_with(
                || black_box(sha2::Sha256::digest(b"hash payload-123ABC@#&".as_slice())),
                TaskCost::High,
            )
            .unwrap();
        } else {
            p.submit_with(
                || black_box(hex::encode(b"hex payload-123ABC@#&".as_slice())),
                TaskCost::Low,
            )
            .unwrap();
        }
    }
    println!(
        "workers stats after submitting {TASK_COUNT} tasks: {:?}",
        p.stats()
    );
    p.wait_all().unwrap();
    let elapsed = start.elapsed();

    let baseline = Duration::from_millis(110);
    assert!(
        elapsed < baseline,
        "perf regression: took {:?} to complete {TASK_COUNT} tasks",
        elapsed
    );
}

#[test]
fn perf_regress_reference_batch() {
    let p = WorkerPool::init_with(8).unwrap();

    const TASK_COUNT: usize = 178956;

    let tasks = (0..TASK_COUNT).map(|_| {
        let is_heavy = fastrand::bool();
        let cost = if is_heavy {
            TaskCost::High
        } else {
            TaskCost::Low
        };

        (
            move || {
                if is_heavy {
                    black_box(sha2::Sha256::digest(b"hash payload-123ABC@#&".as_slice()));
                } else {
                    black_box(hex::encode(b"hex payload-123ABC@#&".as_slice()));
                }
            },
            cost,
        )
    });

    let start = Instant::now();
    drop(p.submit_batch(tasks).unwrap());
    println!(
        "workers stats after submitting {TASK_COUNT} tasks in one batch: {:?}",
        p.stats()
    );
    p.wait_all().unwrap();
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_millis(150),
        "perf regression: took {:?} to complete {TASK_COUNT} tasks in one batch",
        elapsed
    );
}
