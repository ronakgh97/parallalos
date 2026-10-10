use crate::Task;
use crossbeam::channel::Sender;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::Instant;

/// Handle to a worker thread, recv tasks from scheduler
/// and run them until the scheduler is dropped
pub struct WorkerHandle {
    #[allow(unused)]
    pub id: usize,
    pub tx: Sender<Task>,
    pub stats: Arc<WorkerStats>,
}

/// Initialize worker thread `id` and return its `WorkerHandle` and `thread's JoinHandle`
///
/// This spawn thread and run tasks until every `WorkerHandle` is dropped.
/// According to `crossbeam docs`, this will return only if `all senders are dropped` AND `the channel is empty`,
/// therefore this drains the channel even if schedular has drop tx_handles, and return on empty channel.
///
/// Sends one tick on `idle_tx` after completing all task queued to it,
/// so that the pool can wait for `all workers to be idle` and shutdown gracefully.
pub fn init_worker(id: usize, idle_tx: Sender<()>) -> (WorkerHandle, JoinHandle<()>) {
    let (tx, rx) = crossbeam::channel::unbounded::<Task>();
    let stats = Arc::new(WorkerStats {
        total_task_cost: AtomicU64::new(0),
        total_task_executed: AtomicU64::new(0),
        total_task_exec_time: AtomicU64::new(0),
        ewma_exec_time_per_task_cost: AtomicU64::new(32), // zero would cause weird behavior on cold start worker
    });
    let thread_stats = Arc::clone(&stats);
    let thread_handle = std::thread::spawn(move || {
        while let Ok(task) = rx.recv() {
            let start = Instant::now();
            // execute the task, set id by the worker executing it
            // and return value to TaskHandle via oneshot channel
            (task.exec)(id);
            let elapsed = start.elapsed().as_nanos() as u64;
            let cost = task.cost.to_value();
            let drained = thread_stats
                .total_task_cost
                .fetch_sub(cost, Ordering::Relaxed)
                == cost; // this becomes true after executing the last task in the queue
            thread_stats
                .total_task_exec_time
                .fetch_add(elapsed, Ordering::Relaxed);
            thread_stats
                .total_task_executed
                .fetch_add(1, Ordering::Relaxed);
            thread_stats.update_ewa(elapsed, cost);

            // signal the pool that this worker has drained
            // its queue and is idle at this instant
            if drained {
                let _ = idle_tx.send(());
            }
        }
    });
    (WorkerHandle { id, tx, stats }, thread_handle)
}

/// Stats of worker, needed for scheduling tasks to suitable worker thread
pub struct WorkerStats {
    pub total_task_cost: AtomicU64,
    pub total_task_executed: AtomicU64,
    pub total_task_exec_time: AtomicU64,
    pub ewma_exec_time_per_task_cost: AtomicU64,
}

impl WorkerStats {
    /// Update the exponentially weighted `moving average` of execution time
    /// per cost unit, i.e. observed nanos per unit of declared `TaskCost` weight.
    #[inline(always)]
    fn update_ewa(&self, elapsed: u64, cost: u64) {
        // formula; new_ewa = observed variable * alpha + old_ewa * (1 - alpha)
        const ALPHA: f64 = 1.0 / 8.0;
        let variable = ((elapsed + (1 << cost.ilog2()) - 1) >> cost.ilog2()) as f64; // std div_ceil() does "/" and "%" operations
        let current = self.ewma_exec_time_per_task_cost.load(Ordering::Relaxed) as f64;
        let new_ewa = ALPHA * (variable) + (1.0 - ALPHA) * current;
        self.ewma_exec_time_per_task_cost
            .store(new_ewa as u64, Ordering::Relaxed);
    }
}
