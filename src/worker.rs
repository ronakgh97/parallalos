use crate::Job;
use crossbeam::channel::{Receiver, Sender};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::JoinHandle;

/// Handle to a worker thread, recv jobs from scheduler
/// and run them until the scheduler is dropped.
pub struct WorkerHandle {
    pub tx: Sender<Job>,
    pub jobs_load: AtomicU64,
    pub job_executed: AtomicU64,
    pub ewa_execution_time: AtomicU64,
}

pub fn init_worker() -> (WorkerHandle, JoinHandle<()>) {
    let (tx, rx) = crossbeam::channel::unbounded();
    let handle = WorkerHandle {
        tx,
        jobs_load: AtomicU64::new(0),
        job_executed: AtomicU64::new(0),
        ewa_execution_time: AtomicU64::new(0),
    };
    let thread = std::thread::spawn(move || handle.run(rx));
    (handle, thread)
}

impl WorkerHandle {
    /// Run jobs until every `tx_handles` is dropped.
    /// According to docs, this will error only if `all senders are dropped` AND `the channel is empty`,
    /// therefore this drains the channel even if schedular has drop tx_handles and return only if no more jobs are available.
    pub fn run(&self, rx: Receiver<Job>) {
        while let Ok(job) = rx.recv() {
            let start = std::time::Instant::now();
            let load = job.cost;
            {
                self.jobs_load.fetch_add(load, Ordering::Relaxed);
                (job.exec)(); // execute the job, return value to JobHandle via inner channel
                self.jobs_load.fetch_sub(load, Ordering::Relaxed);
            }
            self.job_executed.fetch_add(1, Ordering::Relaxed);
            let elapsed = start.elapsed().as_nanos() as u64;
            self.update_ewa(elapsed);
        }
    }

    /// Update the exponentially weighted average of execution time.
    fn update_ewa(&self, elapsed: u64) {
        // formula: (elapsed * a + old_ewa * (1 - a))
        const ALPHA: f64 = 1.0 / 16.0;
        let current = self.ewa_execution_time.load(Ordering::Relaxed) as f64;
        let new_ewa = ALPHA * (elapsed as f64) + (1.0 - ALPHA) * current;
        self.ewa_execution_time
            .store(new_ewa as u64, Ordering::Relaxed);
    }
}
