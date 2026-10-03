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
            {
                let load = task.cost;
                (task.exec)(); // execute the task, return value to taskHandle via inner channel
                thread_stats.tasks_load.fetch_sub(load, Ordering::Relaxed);
            }
            thread_stats.task_executed.fetch_add(1, Ordering::Relaxed);
            let elapsed = start.elapsed().as_nanos() as u64;
            thread_stats.update_ewa(elapsed);
        }
    });
    (WorkerHandle { tx, stats }, thread_handle)
}

/// Stats of worker, needed to schedular tasks to suitable worker thread
#[derive(Default)]
pub struct WorkerStats {
    pub tasks_load: AtomicU64,
    pub task_executed: AtomicU64,
    pub ewma_execution_time: AtomicU64,
}

impl WorkerStats {
    /// Update the exponentially weighted `moving average` of execution time
    fn update_ewa(&self, elapsed: u64) {
        // formula: (elapsed * a + old_ewa * (1 - a))
        const ALPHA: f64 = 1.0 / 16.0;
        let current = self.ewma_execution_time.load(Ordering::Relaxed) as f64;
        let new_ewa = ALPHA * (elapsed as f64) + (1.0 - ALPHA) * current;
        self.ewma_execution_time
            .store(new_ewa as u64, Ordering::Relaxed);
    }
}
