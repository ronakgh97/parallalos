//! # Parallelos
//!
//! An experimental task scheduler for parallel workloads. Submit independent tasks,
//! let the pool distribute the work, and collect their results when needed.
//!
//! Use [`WorkerPool`] to manage your own pool, or [`parallelize`]
//! to run two tasks using the shared global pool. For workloads with varying task
//! sizes, [`TaskCost`] lets you express their relative cost.
//!
//! ## Example
//!
//! ```
//! use parallelos::pool::WorkerPool;
//!
//! let pool = WorkerPool::init_with(4)?;
//! let a = pool.submit(|| (1..=10u64).sum::<u64>())?;
//! let b = pool.submit(|| (1..=10u64).product::<u64>())?;
//!
//! let (sum, _) = a.wait()?;
//! let (product, _) = b.wait()?;
//! assert_eq!((sum, product), (55, 3_628_800));
//! # Ok::<(), anyhow::Error>(())
//! ```
//!
//! **Experimental:** the API and scheduling behavior may change. Nested parallelism
//! is not supported; waiting on work in the same pool from within a task can deadlock.

use crate::pool::WorkerPool;
use crate::tasks::{TaskCost, TaskHandle};
use std::sync::atomic::Ordering;

pub mod pool;
pub mod tasks;
pub(crate) mod worker;

/// A unit of work to be executed by a worker thread
pub(crate) struct Task {
    /// Enum that denotes how "costly" the task is
    pub cost: TaskCost,
    /// Closure that capture callable and worker id that's executing it
    pub exec: Box<dyn FnOnce(usize) + Send + 'static>,
}

pub(crate) static GLOBAL_WORKER_POOL: std::sync::LazyLock<WorkerPool> =
    std::sync::LazyLock::new(|| {
        WorkerPool::init().expect("failed to initialize global worker pool")
    });

/// Initializes the global worker pool.
pub fn init_global_pool() {
    let _ = &GLOBAL_WORKER_POOL;
}

/// Returns a reference to the global worker pool.
pub fn get_global_pool() -> &'static WorkerPool {
    init_global_pool();
    &GLOBAL_WORKER_POOL
}

/// Executes two tasks in parallel, blocks and returning their results.
pub fn parallelize<A, B, RA, RB>(
    task_a: A,
    cost_a: TaskCost,
    task_b: B,
    cost_b: TaskCost,
) -> anyhow::Result<(RA, RB)>
where
    A: FnOnce() -> RA + Send + 'static,
    B: FnOnce() -> RB + Send + 'static,
    RA: Send + 'static,
    RB: Send + 'static,
{
    let pool = get_global_pool();
    let cost_a_value = cost_a.to_value();
    let cost_b_value = cost_b.to_value();

    let first = pool
        .worker_handles
        .iter()
        .min_by_key(|w| w.stats.total_task_cost.load(Ordering::Relaxed))
        .ok_or(anyhow::anyhow!("global_pool is not initialized"))?; // should never happen, since pool can't be dropped

    let second = pool
        .worker_handles
        .iter()
        .filter(|w| w.id != first.id) // pool always have at least 2 workers, since
        .min_by_key(|w| w.stats.total_task_cost.load(Ordering::Relaxed))
        .ok_or(anyhow::anyhow!("global_pool is not initialized"))?;

    // assign costly task to least loaded worker
    if cost_a_value < cost_b_value {
        let hb = pool.submit_to_worker(first.id, task_b, cost_b)?;
        let ha = pool.submit_to_worker(second.id, task_a, cost_a)?;
        Ok((ha.wait()?.0, hb.wait()?.0))
    } else if cost_a_value > cost_b_value {
        let ha = pool.submit_to_worker(first.id, task_a, cost_a)?;
        let hb = pool.submit_to_worker(second.id, task_b, cost_b)?;
        Ok((ha.wait()?.0, hb.wait()?.0))
    } else {
        let coin_flip = fastrand::bool();
        if coin_flip {
            let ha = pool.submit_to_worker(first.id, task_a, cost_a)?;
            let hb = pool.submit_to_worker(second.id, task_b, cost_b)?;
            Ok((ha.wait()?.0, hb.wait()?.0))
        } else {
            let hb = pool.submit_to_worker(first.id, task_b, cost_b)?;
            let ha = pool.submit_to_worker(second.id, task_a, cost_a)?;
            Ok((ha.wait()?.0, hb.wait()?.0))
        }
    }
}

/// Spawns a task with the given cost in the global worker pool.
/// This is a convenience wrapper around `WorkerPool::submit_with` using the global pool.
pub fn spawn<F: FnOnce() + Send + 'static>(
    task: F,
    cost: TaskCost,
) -> anyhow::Result<TaskHandle<()>> {
    let pool = get_global_pool();
    pool.submit_with(task, cost)
}

#[test]
fn lib_test() {
    init_global_pool();
    let pool = WorkerPool::init_with(4).unwrap();
    let counter = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));

    let task_handles: Vec<_> = (0..100)
        .map(|_| {
            let counter_clone = std::sync::Arc::clone(&counter);
            pool.submit(move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
            })
        })
        .collect();

    for handle in task_handles {
        let _ = handle.unwrap().wait();
    }
    // pool.wait_all_tasks();

    assert_eq!(counter.load(Ordering::SeqCst), 100);

    let time_taken = std::time::Instant::now();
    parallelize(
        || {
            std::thread::sleep(std::time::Duration::from_millis(200));
        },
        TaskCost::Normal,
        || {
            std::thread::sleep(std::time::Duration::from_millis(100));
        },
        TaskCost::Normal,
    )
    .unwrap();
    let elapsed = time_taken.elapsed();
    assert!(elapsed < std::time::Duration::from_millis(225));
}
