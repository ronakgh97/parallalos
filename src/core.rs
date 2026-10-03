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

/// Snapshot of a worker's current state, used for monitoring and scheduling decisions
pub struct WorkerState {
    pub task_load: u64,
    pub task_queued: u64,
    pub task_executed: u64,
    pub ewa_execution_time: u64,
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
        let mut best_ties = 0u32;
        let mut best_load = u64::MAX;
        let mut best_time = u64::MAX;

        for (i, worker) in self.worker_handles.iter().enumerate() {
            let load = worker.stats.task_load.load(Ordering::Relaxed);
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
                    if rand::random_ratio(1, best_ties) {
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

    // TODO: fn submit_batch()

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
        let cost = cost.clamp(1, 1 << 20); // 1048576 is to avoid overflow in fetch_add

        // safety: we already checked that index is valid, this is free perf
        let worker_tx_handle = unsafe { self.worker_handles.get_unchecked(index) };

        worker_tx_handle
            .stats
            .task_load
            .fetch_add(cost, Ordering::Relaxed);
        let (rtx, rrx) = bounded(1);
        let task = Task {
            cost,
            exec: exec_task!(f, rtx),
        };

        // send the task to selected worker queue
        if let Err(_) = worker_tx_handle.tx.send(task) {
            // avoid leaking task load
            // if the worker has been dropped somehow maybe
            worker_tx_handle
                .stats
                .task_load
                .fetch_sub(cost, Ordering::Relaxed);
            return Err(anyhow!("worker pool has been shut down"));
        }

        // hand the result_rx to the caller, so they can wait for the result
        Ok(TaskHandle { rx: rrx })
    }

    /// Returns the current states for the worker in the pool
    pub fn stats(&self) -> Vec<WorkerState> {
        self.worker_handles
            .iter()
            .map(|w| WorkerState {
                task_load: w.stats.task_load.load(Ordering::Relaxed),
                task_queued: w.tx.len() as u64,
                task_executed: w.stats.task_executed.load(Ordering::Relaxed),
                ewa_execution_time: w.stats.ewma_execution_time.load(Ordering::Relaxed),
            })
            .collect()
    }

    /// Stops accepting task, waits for all queued tasks to complete
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

    // TODO: fn shutdown_timeout()
}

impl Drop for Pool {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
