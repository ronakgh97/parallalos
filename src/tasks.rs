use anyhow::{Result, anyhow};
use crossbeam::channel::Receiver;
use std::any::Any;

/// Result of a submitted task, as reported back by the worker that ran it
pub(crate) enum TaskResult<T> {
    /// The task ran successfully containing the return value
    Success(T),
    /// The task panicked during execution, containing the panic payload
    Panic(Box<dyn Any + Send + 'static>), // from std::thread::Result
}

/// Handle to a submitted task, used to await its result
pub struct TaskHandle<T> {
    pub(crate) rx: Receiver<TaskResult<T>>,
}

impl<T> TaskHandle<T> {
    /// Blocks until the task reports a result.
    ///
    /// If the task panics, the panic is `re-raised` in the waiting thread.
    /// rather than returned as an error using `std::panic::catch_unwind` and `std::panic::resume_unwind`.
    pub fn wait(self) -> Result<T> {
        match self.rx.recv() {
            Ok(TaskResult::Success(s)) => Ok(s),
            Ok(TaskResult::Panic(e)) => std::panic::resume_unwind(e),
            Err(e) => Err(anyhow!("worker failed to send task result: {e}")),
        }
    }
}
