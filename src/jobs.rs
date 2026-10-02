use anyhow::{Error, Result};
use crossbeam::channel::Receiver;
use std::any::Any;

// Represents the result of a job execution
enum JobResult<T> {
    /// The job completed successfully with a result of type T
    Success(T),
    /// The job was interrupted before completion
    Interrupt,
    /// The job panicked during execution, containing the panic payload
    Panic(Box<dyn Any + Send + 'static>), // from std::thread::Result
}

// Represents a handle to a submitted job,
// allowing the caller to wait for its completion and retrieve the result
pub struct JobHandle<T> {
    rx: Receiver<JobResult<T>>,
}

impl<T> JobHandle<T> {
    /// Waits for the job to complete and returns the result
    pub fn wait(self) -> Result<T> {
        match self.rx.recv() {
            Ok(JobResult::Success(s)) => Ok(s),
            Ok(JobResult::Interrupt) => Err(anyhow::anyhow!("Job was interrupted")),
            Ok(JobResult::Panic(e)) => std::panic::resume_unwind(e),
            Err(e) => panic!("Failed to receive job result: {}", e),
        }
    }
}
