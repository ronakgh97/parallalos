use crate::Job;
use crate::jobs::JobHandle;
use crate::worker::WorkerHandle;
use anyhow::Result;
use std::sync::mpsc::Sender;

pub struct Pool {
    workers: Box<[WorkerHandle]>,
}

impl Pool {
    pub fn init() -> Result<Self> {
        let cores_available = std::thread::available_parallelism()?.get();
        Self::init_with(cores_available)
    }

    pub fn init_with(n: usize) -> Result<Self> {
        let worker_handles: Vec<Sender<Job>> = (0..n)
            .map(|_| {
                let (worker, tx) = WorkerHandle::init();
                std::thread::spawn(move || worker.run_jobs());
                tx
            })
            .collect::<Result<_, _>>()?;
        Ok(Pool {
            workers: worker_handles.into_boxed_slice(),
        })
    }

    pub fn submit(&self) -> JobHandle {}

    pub fn submit_with_cost() -> JobHandle {}

    pub fn stats() {}

    pub fn shutdown(&mut self) {}
}
