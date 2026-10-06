use crate::Task;
use crossbeam::channel::Sender;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::JoinHandle;

/// Handle to a worker thread, recv tasks from scheduler
/// and run them until the scheduler is dropped
pub struct WorkerHandle {
    pub tx: Sender<Task>,
    pub stats: Arc<WorkerStats>,
}

/// Initialize a worker thread and return its `WorkerHandle` and `thread's JoinHandle`
///
/// This spawn thread and run tasks until every `WorkerHandle` is dropped.
/// According to `crossbeam docs`, this will return only if `all senders are dropped` AND `the channel is empty`,
/// therefore this drains the channel even if schedular has drop tx_handles, and return on empty channel.
pub fn init_worker() -> (WorkerHandle, JoinHandle<()>) {
    let (tx, rx) = crossbeam::channel::unbounded::<Task>();
    let stats = Arc::from(WorkerStats::default());
    let thread_stats = Arc::clone(&stats);
    let thread_handle = std::thread::spawn(move || {
        while let Ok(task) = rx.recv() {
            let start = std::time::Instant::now();
            (task.exec)(); // execute the task, return value to taskHandle via inner channel
            let elapsed = start.elapsed().as_nanos() as u64;
            thread_stats
                .total_task_cost
                .fetch_sub(task.cost.to_value(), Ordering::Relaxed);
            thread_stats
                .total_task_executed
                .fetch_add(1, Ordering::Relaxed);
            thread_stats.update_ewa(elapsed, task.cost.to_value());
        }
    });
    (WorkerHandle { tx, stats }, thread_handle)
}

/// Stats of worker, needed for scheduling tasks to suitable worker thread
#[derive(Default)]
pub struct WorkerStats {
    pub total_task_cost: AtomicU64,
    pub total_task_executed: AtomicU64,
    pub ewma_exec_time_per_task: AtomicU64,
}

impl WorkerStats {
    /// Update the exponentially weighted `moving average` of execution time
    /// per cost unit, i.e. observed nanos per unit of declared `TaskCost` weight.
    #[inline(always)]
    fn update_ewa(&self, elapsed: u64, cost: u64) {
        // formula; new_ewa = observed variable * alpha + old_ewa * (1 - alpha)
        const ALPHA: f64 = 1.0 / 8.0;
        let variable = ((elapsed + (1 << cost.ilog2()) - 1) >> cost.ilog2()) as f64; // div_ceil(elapsed / cost)
        let current = self.ewma_exec_time_per_task.load(Ordering::Relaxed) as f64;
        let new_ewa = ALPHA * (variable) + (1.0 - ALPHA) * current;
        self.ewma_exec_time_per_task
            .store(new_ewa as u64, Ordering::Relaxed);
    }
}
