use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::Sender;

pub mod core;
pub(crate) mod jobs;
pub(crate) mod worker;

/// Represents a job to be executed by the thread pool
pub(crate) struct Job {
    pub cost: usize,
    pub f: Box<dyn FnOnce() + Send + 'static>,
}

/// State shared between the pool and its worker threads
pub(crate) struct SharedState {
    pub tx_handles: Arc<[Sender<Job>]>,
    pub shutdown_flag: AtomicBool,
}
