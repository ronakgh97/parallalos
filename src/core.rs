use crate::Task;
use crate::tasks::{TaskHandle, TaskResult};
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
pub struct Pool {
    worker_handles: Vec<WorkerHandle>,
    worker_threads: Vec<JoinHandle<()>>,
}

impl Pool {
    /// Creates a new worker pool with the number of threads equal to the number of available CPU cores
    pub fn init() -> Result<Self> {
        Self::init_with(std::thread::available_parallelism()?.get())
    }

    /// Creates a new worker pool with the specified number of threads
    pub fn init_with(n: usize) -> Result<Self> {
        let (handles, threads) = (0..n)
            .into_iter()
            .map(|_| {
                let (handle, thread) = init_worker();
                (handle, thread)
            })
            .collect();

        Ok(Pool {
            worker_handles: handles,
            worker_threads: threads,
        })
    }

    /// Returns the index of worker for assigning the task
    #[inline(always)]
    fn schedule_worker(&self) -> Option<usize> {
        let n = self.worker_handles.len();
        if n == 0 {
            return None;
        }

        // TODO: improve this later
        let mut best_idx = 0;
        let mut best_ties = 0u64;
        let mut best_load = u64::MAX;
        let mut best_time = u64::MAX;

        for (i, worker) in self.worker_handles.iter().enumerate() {
            let load = worker.stats.tasks_load.load(Ordering::Relaxed);
            let time = worker.stats.ewma_execution_time.load(Ordering::Relaxed);

            match (load, time).cmp(&(best_load, best_time)) {
                std::cmp::Ordering::Less => {
                    best_load = load;
                    best_time = time;
                    best_idx = i;
                    best_ties = 1;
                }

                std::cmp::Ordering::Equal => {
                    best_ties += 1;

                    // randomly select one of the tied workers
                    // probability is same for each worker
                    let chance = 1.0 / (best_ties as f64);
                    if rand::random_bool(chance) {
                        best_idx = i;
                    }
                }

                std::cmp::Ordering::Greater => { /*do nothing*/ }
            }
        }

        Some(best_idx)
    }

    /// Submits a task to the pool and returns a handle to await its result
    pub fn submit<F, T>(&self, f: F) -> Result<TaskHandle<T>>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.submit_with_cost(f, 1)
    }

    /// Submits a task to the pool with an approximate execution cost
    /// and returns a handle for awaiting its result
    pub fn submit_with_cost<F, T>(&self, f: F, cost: u64) -> Result<TaskHandle<T>>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let index = self
            .schedule_worker()
            .ok_or_else(|| anyhow!("worker pool has been shut down"))?;
        let cost = cost.clamp(1, 1 << 20);

        self.worker_handles[index]
            .stats
            .tasks_load
            .fetch_add(cost, Ordering::Relaxed);
        let (rtx, rrx) = bounded(1);
        let task = Task {
            cost,
            exec: exec_task!(f, rtx),
        };

        // send the task to selected worker queue
        self.worker_handles[index]
            .tx
            .send(task)
            .map_err(|_| anyhow!("worker has been stopped"))?;

        // hand the result_rx to the caller, so they can wait for the result
        Ok(TaskHandle { rx: rrx })
    }

    /// Returns the current statistics for the worker pool.
    pub fn stats(&self) {}

    /// Stops accepting work, waits for all queued tasks to complete.
    pub fn shutdown(&mut self) -> Result<()> {
        // drop all senders, workers will exit when their queues drain
        self.worker_handles.clear();

        let mut thread_panicked = 0u64;
        for threads in self.worker_threads.drain(..) {
            if let Err(_) = threads.join() {
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
}

impl Drop for Pool {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
