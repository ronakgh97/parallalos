use anyhow::{Result, anyhow};
use crossbeam::channel::Receiver;
use std::any::Any;

/// Result of a submitted job, as reported back by the worker that ran it
pub(crate) enum JobResult<T> {
    /// The job ran successfully containing the return value
    Success(T),
    /// The job panicked during execution, containing the panic payload
    Panic(Box<dyn Any + Send + 'static>), // from std::thread::Result
}

/// Handle to a submitted job, used to await its result
pub struct JobHandle<T> {
    rx: Receiver<JobResult<T>>,
}

impl<T> JobHandle<T> {
    /// Blocks until the job reports a result.
    /// Panics are `re-raised` in the waiting thread
    /// rather than returned as an error using `std::panic::catch_unwind` and `std::panic::resume_unwind`.
    pub fn wait(self) -> Result<T> {
        match self.rx.recv() {
            Ok(JobResult::Success(s)) => Ok(s),
            Ok(JobResult::Panic(e)) => std::panic::resume_unwind(e),
            Err(e) => Err(anyhow!("worker failed to send job result: {e}")),
        }
    }
}
