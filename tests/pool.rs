use parallelos::core::Pool;
use parallelos::tasks::TaskHandle;
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

fn assert_balanced(p: &Pool) {
    assert!(
        wait_until(|| p.stats().iter().all(|w| w.task_load == 0)),
        "task_load never balanced: {:?}",
        p.stats()
    );
}

// --- load accounting

#[test]
fn loads_balance_after_all_tasks_complete() {
    let p = Pool::init_with(4).unwrap();
    let handles: Vec<TaskHandle<u64>> = (1..=64u64)
        .map(|c| p.submit_with_cost(move || c * c, c).unwrap())
        .collect();

    for h in handles {
        h.wait().unwrap();
    }

    assert!(
        p.stats().iter().all(|w| w.task_load == 0),
        "unbalanced loads: {:?}",
        p.stats()
    );
}

#[test]
fn fire_and_forget_tasks_still_balance() {
    let p = Pool::init_with(4).unwrap();
    // drop every handle immediately; nobody waits.
    for c in 1..=64u64 {
        let _ = p.submit_with_cost(move || c, c);
    }
    assert_balanced(&p);
}

#[test]
fn cost_is_clamped_so_load_never_overflows() {
    let p = Pool::init_with(1).unwrap();
    // Without clamping, two fetch_add(u64::MAX) overflows and panics in debug.
    p.submit_with_cost(|| 1u64, u64::MAX)
        .unwrap()
        .wait()
        .unwrap();
    p.submit_with_cost(|| 2u64, u64::MAX)
        .unwrap()
        .wait()
        .unwrap();
    assert_balanced(&p);
}

// --- results and panics

#[test]
fn every_task_returns_its_value() {
    let p = Pool::init_with(4).unwrap();
    let sum: u64 = (0..64u64)
        .map(|i| p.submit(move || i * i).unwrap())
        .map(|h| h.wait().unwrap())
        .sum();

    assert_eq!(sum, (0..64u64).map(|i| i * i).sum());
}

#[test]
fn a_panicking_task_does_not_kill_its_worker() {
    let p = Pool::init_with(1).unwrap();

    let h = p.submit(|| -> () { panic!("boom") }).unwrap();
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = h.wait();
    }));
    assert!(caught.is_err(), "panic should cross the thread boundary");

    assert_eq!(p.submit(|| 7u64).unwrap().wait().unwrap(), 7);
    assert_balanced(&p);
}

// --- shutdown and Drop

#[test]
fn submit_after_shutdown_is_refused() {
    let mut p = Pool::init_with(2).unwrap();
    p.shutdown().unwrap();
    assert!(p.submit(|| 1).is_err());
}

#[test]
fn drop_without_shutdown_still_completes_tasks() {
    let p = Pool::init_with(2).unwrap();
    let h = p.submit(|| 42u64).unwrap();
    drop(p); // Drop runs shutdown(), which drains the queue and runs the task.
    assert_eq!(h.wait().unwrap(), 42);
}

#[test]
fn shutdown_drains_queued_tasks_without_waiting() {
    let mut p = Pool::init_with(4).unwrap();
    for _ in 0..200 {
        let _ = p.submit(|| ());
    }
    // A drain failure shows up as a hang or an Err here.
    p.shutdown().unwrap();
}

// --- contention

#[test]
fn concurrent_submit_balances() {
    let p = Arc::new(Pool::init_with(4).unwrap());

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

    let actual: u64 = handles.into_iter().map(|h| h.wait().unwrap()).sum();
    assert_eq!(actual, expected);

    assert!(
        wait_until(|| p.stats().iter().all(|w| w.task_load == 0)),
        "task_load never balanced: {:?}",
        p.stats()
    );
}
