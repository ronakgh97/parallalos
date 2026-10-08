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
            p.submit_with_cost(move || i * i, c).unwrap()
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
        let _ = p.submit_with_cost(move || i, c);
    }
    assert_balanced(&p);
}

#[test]
fn low_cost_tasks_count_toward_load() {
    let p = WorkerPool::init_with(2).unwrap();
    p.submit_with_cost(|| 1u64, TaskCost::Low)
        .unwrap()
        .wait()
        .unwrap();
    p.submit_with_cost(|| 2u64, TaskCost::Low)
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

// Reference perf point for later refactors, not a correctness test
#[test]
fn perf_regress_reference() {
    let p = WorkerPool::init_with(8).unwrap();

    const TASK_COUNT: usize = 178956; // MAGIC NUMBER, DO NOT CHANGE!!!

    let start = Instant::now();
    for _ in 0..TASK_COUNT {
        if fastrand::bool() {
            p.submit_with_cost(
                || black_box(sha2::Sha256::digest(b"hash payload-123ABC@#&".as_slice())),
                TaskCost::High,
            )
            .unwrap();
        } else {
            p.submit_with_cost(
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
    p.wait_all_tasks();
    let elapsed = start.elapsed();

    let baseline = Duration::from_millis(110);
    assert!(
        elapsed < baseline,
        "perf regression: took {:?} to complete {TASK_COUNT} tasks",
        elapsed
    );
}
