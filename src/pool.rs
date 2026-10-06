use crate::Task;
use crate::tasks::{TaskCost, TaskHandle, TaskResult};
use crate::worker::{WorkerHandle, init_worker};
use anyhow::{Result, anyhow};
use crossbeam::channel::bounded;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;

/// Packs a callable function and a channel sender into a closure
/// that runs the task and sends back the result
macro_rules! exec_task {
    ($f:expr, $rtx:expr) => {
        Box::new(move || {
            let result = match catch_unwind(AssertUnwindSafe($f)) {
                Ok(v) => TaskResult::Success(v),
                Err(e) => TaskResult::Panic(e),
            };

            let _ = $rtx.send(result);
        })
    };
}

/// A pool of worker threads that can schedule/execute tasks concurrently
pub struct WorkerPool {
    worker_handles: Vec<WorkerHandle>,
    worker_threads: Vec<JoinHandle<()>>,
}

/// Snapshot of a worker's current state, used for `monitoring` and `scheduling` decisions
#[derive(Debug, Clone, Copy)]
pub struct WorkerState {
    pub tasks_cost: u64,
    pub tasks_queued: u64,
    pub tasks_executed: u64,
    pub ewa_exec_time_per_task: u64,
    pub predicted_completion_time: u64,
}

impl WorkerPool {
    /// Creates a new worker pool with the number of threads equal to the `number of available CPU cores`.
    pub fn init() -> Result<Self> {
        let thread_count = std::thread::available_parallelism()?.get();
        assert!(thread_count > 1, "worker pool must have at least 2 threads");
        Self::init_with(thread_count)
    }

    /// Creates a new worker pool with the `specified number of threads`.
    pub fn init_with(n: usize) -> Result<Self> {
        assert!(n > 1, "worker pool must have at least 2 threads");
        let (handles, threads) = (0..n)
            .into_iter()
            .map(|_| {
                let (handle, thread) = init_worker();
                (handle, thread)
            })
            .collect();

        Ok(WorkerPool {
            worker_handles: handles,
            worker_threads: threads,
        })
    }

    /// Returns the index of worker for assigning the task.
    #[inline(always)]
    fn schedule_worker(&self) -> Option<usize> {
        let n = self.worker_handles.len();
        if n == 0 {
            return None;
        }

        // rand sample two distinct workers and pick the one
        // with the lesser `total_tasks_cost (queued/running) * ewma_execution_time_per_task_cost`
        // i.e. find the worker that is predicted to complete it task faster
        let a = fastrand::usize(..n);
        let b = {
            let b = fastrand::usize(..n - 1);
            if b >= a { b + 1 } else { b }
        };

        // the rank is a predicted time (in nanos) to drain this worker task queue
        let (wa, wb) = (&self.worker_handles[a], &self.worker_handles[b]);
        let rank_a = wa
            .stats
            .total_task_cost
            .load(Ordering::Relaxed)
            .saturating_mul(wa.stats.ewma_exec_time_per_task.load(Ordering::Relaxed));

        let rank_b = wb
            .stats
            .total_task_cost
            .load(Ordering::Relaxed)
            .saturating_mul(wb.stats.ewma_exec_time_per_task.load(Ordering::Relaxed));

        if rank_a > rank_b {
            Some(b)
        } else if rank_a < rank_b {
            Some(a)
        } else {
            let coin_flip = fastrand::bool(); // 50/50 on tie
            if coin_flip { Some(a) } else { Some(b) }
        }
    }

    // TODO: fn add_worker()

    // TODO: fn remove_worker()

    /// Submits a task to the pool with `TaskCost::Normal` and returns a handle to await its result.
    ///
    /// Use `submit_with_cost` if you want to specify a different cost for the task.
    pub fn submit<F, T>(&self, f: F) -> Result<TaskHandle<T>>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.submit_with_cost(f, TaskCost::Normal)
    }

    // TODO: fn submit_batch()

    /// Submits a task to the pool with an `TaskCost`,
    /// i.e. how heavy the task is expected to be, in range of `Normal` to `VeryHigh`
    /// and returns a handle for awaiting its result or errors if Pool is shut down.
    ///
    /// > TaskCost affecting behavior of schedular will change or removed in the future
    pub fn submit_with_cost<F, T>(&self, f: F, cost: TaskCost) -> Result<TaskHandle<T>>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let index = self
            .schedule_worker()
            .ok_or_else(|| anyhow!("worker pool has been shut down"))?;
        let cost_value = cost.to_value();

        let worker_tx_handle = &self.worker_handles[index];
        worker_tx_handle
            .stats
            .total_task_cost
            .fetch_add(cost_value, Ordering::Relaxed);

        let (rtx, rrx) = bounded(1);
        let task = Task {
            cost,
            exec: exec_task!(f, rtx),
        };

        // send the task to selected worker queue
        if worker_tx_handle.tx.send(task).is_err() {
            // task was dropped, release the charge that `task_load` was tracking
            worker_tx_handle
                .stats
                .total_task_cost
                .fetch_sub(cost_value, Ordering::Relaxed);
            return Err(anyhow!("worker pool has been shut down"));
        }

        // hand the result_rx to the caller, so they can wait for the result
        Ok(TaskHandle { rx: rrx })
    }

    /// Waits for all submitted tasks to complete.
    ///
    /// This method `blocks until all tasks submitted before/after` this call are completed.
    /// It does not block `submit` calls that happen after this call.
    pub fn wait_all_tasks(&self) {
        loop {
            let total_load: u64 = self
                .worker_handles
                .iter()
                .map(|w| w.stats.total_task_cost.load(Ordering::Relaxed))
                .sum(); // TODO: race condition here

            // block for all worker load to drain to zero
            if total_load == 0 {
                break;
            }

            std::thread::yield_now(); // TODO: busy-wait here
        }
    }

    /// Returns the current states for the worker in the pool.
    pub fn stats(&self) -> Vec<WorkerState> {
        self.worker_handles
            .iter()
            .map(|w| WorkerState {
                tasks_queued: w.tx.len() as u64,
                tasks_cost: w.stats.total_task_cost.load(Ordering::Relaxed),
                tasks_executed: w.stats.total_task_executed.load(Ordering::Relaxed),
                ewa_exec_time_per_task: w.stats.ewma_exec_time_per_task.load(Ordering::Relaxed),
                predicted_completion_time: w
                    .stats
                    .total_task_cost
                    .load(Ordering::Relaxed)
                    .saturating_mul(w.stats.ewma_exec_time_per_task.load(Ordering::Relaxed)),
            })
            .collect()
    }

    /// Stops accepting `submit` calls, waits for all queued tasks to complete.
    pub fn shutdown(&mut self) -> Result<()> {
        // drop all senders, workers will exit when their queues drain
        self.worker_handles.clear();

        let mut thread_panicked = 0u64;
        for threads in self.worker_threads.drain(..) {
            if threads.join().is_err() {
                // eprintln!("worker thread panicked: {:?}", e);
                thread_panicked += 1;
            }
        }

        if thread_panicked > 0 {
            return Err(anyhow!(
                "{} worker threads panicked during shutdown",
                thread_panicked
            ));
        }
        Ok(())
    }

    // TODO: fn shutdown_timeout()
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
