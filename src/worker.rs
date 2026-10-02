use crate::{Job, SharedState};
use crossbeam::channel::{Receiver, Sender};
pub struct WorkerHandle {
    state: SharedState,
    rx_handle: Receiver<Job>,
}

impl WorkerHandle {
    pub fn init() -> (Self, Sender<Job>) {
        let (tx, rx) = crossbeam::channel::unbounded();
        (
            WorkerHandle {
                state: SharedState {
                    tx_handles: Arc::new([]),
                    shutdown_flag: Default::default(),
                },
                rx_handle: rx,
            },
            tx,
        )
    }

    pub fn run_jobs(&self) {
        while let Ok(job) = self.rx_handle.recv() {
            job.f()
        }
    }
}
