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

// TODO:
// /// The priority of a task, which affects the `chances of it being scheduled` to a worker thread,
// /// such that it `better chance of faster completion`
// pub enum TaskPriority {
//     Low,
//     Normal,
//     High,
// }

// TODO:
/// The approximate cost of a task, helps the scheduler to `balance the load` and
/// fairly schedule it, such that `tasks with different workload can together process faster`, and the pool can `maximize throughput`
///
/// To put simply, cost adds weight to `predicted completion time` of workers,
/// so that the scheduler can `avoid overloading` a worker with `high cost tasks` and `fairly distribute` the workload across all workers
///
/// > This will be removed or changed in the future for more improved scheduling and better workload distribution
pub enum TaskCost {
    Low,
    Normal,
    Moderate,
    High,
    VeryHigh,
}

impl TaskCost {
    #[inline(always)]
    pub fn to_value(&self) -> u64 {
        match self {
            TaskCost::Low => 1,        // 2^0
            TaskCost::Normal => 4,     // 2^2
            TaskCost::Moderate => 16,  // 2^4
            TaskCost::High => 64,      // 2^6
            TaskCost::VeryHigh => 256, // 2^8
        }
    }
}

impl<T> TaskHandle<T> {
    /// Blocks until the task reports a result.
    ///
    /// If the task panics, the panic is `re-raised` in the waiting thread,
    /// rather than returned as an error using `std::panic::catch_unwind` and `std::panic::resume_unwind`.
    pub fn wait(self) -> Result<T> {
        match self.rx.recv() {
            Ok(TaskResult::Success(s)) => Ok(s),
            Ok(TaskResult::Panic(e)) => std::panic::resume_unwind(e),
            // sender has been dropped, worker thread has panicked or exited
            Err(e) => Err(anyhow!("worker failed to send task result: {e}")),
        }
    }
}
