use crate::Job;
use crossbeam::channel::Receiver;

/// Handle to a worker thread, recv jobs from scheduler
/// and run them until the scheduler is dropped.
pub struct WorkerHandle {
    pub rx: Receiver<Job>,
}

impl WorkerHandle {
    /// Run jobs until every `tx_handles` is dropped
    pub fn run(&self) {
        while let Ok(job) = self.rx.recv() {
            (job.f)()
        }
    }
}
