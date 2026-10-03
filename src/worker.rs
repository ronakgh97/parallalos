use crate::Job;
use crossbeam::channel::{Receiver, Sender};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::JoinHandle;

/// Handle to a worker thread, recv jobs from scheduler
/// and run them until the scheduler is dropped.
pub struct WorkerHandle {
    pub tx: Sender<Job>,
    pub stats: Arc<WorkerStats>,
}

/// Stats of worker, needed to schedular jobs to suitable worker thread.
#[derive(Default)]
pub struct WorkerStats {
    pub jobs_load: AtomicU64,
    pub job_executed: AtomicU64,
    pub ewa_execution_time: AtomicU64,
}

impl WorkerStats {
    /// Update the exponentially weighted `moving average` of execution time.
    fn update_ewa(&self, elapsed: u64) {
        // formula: (elapsed * a + old_ewa * (1 - a))
        const ALPHA: f64 = 1.0 / 16.0;
        let current = self.ewa_execution_time.load(Ordering::Relaxed) as f64;
        let new_ewa = ALPHA * (elapsed as f64) + (1.0 - ALPHA) * current;
        self.ewa_execution_time
            .store(new_ewa as u64, Ordering::Relaxed);
    }
}

/// Initialize a worker thread and return its `worker handle` and `thread handle`.
pub fn init_worker() -> (WorkerHandle, JoinHandle<()>) {
    let (tx, rx) = crossbeam::channel::unbounded();
    let stats = Arc::from(WorkerStats::default());
    let thread_stats = Arc::clone(&stats);
    let thread_handle = std::thread::spawn(move || run_worker(rx, thread_stats));
    (WorkerHandle { tx, stats }, thread_handle)
}

/// Run jobs until every `tx_handles` is dropped.
/// According to docs, this will return only if `all senders are dropped` AND `the channel is empty`,
/// therefore this drains the channel even if schedular has drop tx_handles, and return on empty channel.
pub fn run_worker(rx_handle: Receiver<Job>, worker_stats: Arc<WorkerStats>) {
    while let Ok(job) = rx_handle.recv() {
        let start = std::time::Instant::now();
        let load = job.cost;
        {
            worker_stats.jobs_load.fetch_add(load, Ordering::Relaxed);
            (job.exec)(); // execute the job, return value to JobHandle via inner channel
            worker_stats.jobs_load.fetch_sub(load, Ordering::Relaxed);
        }
        worker_stats.job_executed.fetch_add(1, Ordering::Relaxed);
        let elapsed = start.elapsed().as_nanos() as u64;
        worker_stats.update_ewa(elapsed);
    }
}
