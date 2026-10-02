use crate::{Job, SharedState};
use crate::jobs::JobHandle;
use crate::worker::WorkerHandle;
use anyhow::Result;
use std::sync::Arc;
use std::thread::JoinHandle;

pub struct Pool {
    shared: Arc<SharedState>,
    threads: Arc<[JoinHandle<()>]>,
}

impl Pool {
    pub fn init() -> Result<Self> {
        let cores_available = std::thread::available_parallelism()?.get();
        Self::init_with(cores_available)
    }

    pub fn init_with(n: usize) -> Result<Self> {
        let worker_handles: Vec<JoinHandle<()>> = (0..n)
            .map(|_| {
                let (worker, tx) = WorkerHandle::init();
                let handle = std::thread::spawn(move || worker.run_jobs());
                tx
            })
        Ok(Pool {
            shared: Arc::new(SharedState { tx: Arc::new([]), shutdown_flag: Default::default() }),
            threads: Arc::from(worker_handles),
        })
    }

    pub fn submit(&self) -> JobHandle {}

    pub fn submit_with_cost() -> JobHandle {}

    pub fn stats() {}

    pub fn shutdown(&mut self) {}
}

enum PoolError {
    Closed,
    ThreadPanic,
}
