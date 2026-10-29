use crate::Task;
use crate::tasks::{TaskCost, TaskHandle, TaskResult};
use crate::worker::{WorkerHandle, init_worker};
use anyhow::{Result, anyhow};
use std::cmp::Ordering as CmpOrdering;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;

/// Packs a callable function, a state reference, and a channel sender into a closure
/// that runs the task and sends back the result
macro_rules! exec_task {
    ($task:expr, $rtx:expr) => {
        Box::new(move |state: &mut S, worker_id: usize| {
            let result = match catch_unwind(AssertUnwindSafe(|| $task(state))) {
                Ok(v) => TaskResult::Success(v, worker_id),
                Err(e) => TaskResult::Panic(e),
            };

            let _ = $rtx.send(result);
        })
    };
}

/// A pool of worker threads with optional worker-owned state, that can schedule/execute tasks concurrently
pub struct WorkerPool<S: 'static = ()> {
    pub(crate) worker_handles: Box<[WorkerHandle<S>]>,
    pub(crate) worker_threads: Box<[JoinHandle<()>]>,
    // this is needed for keeping channel alive, throughout the lifetime of the pool
    #[allow(unused)]
    idle_tx: crossbeam::channel::Sender<()>,
    idle_rx: crossbeam::channel::Receiver<()>,
}

/// Snapshot of a worker's current state, used for `monitoring` and `scheduling` decisions
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct WorkerState {
    pub tasks_cost: u64,
    pub tasks_queued: u64,
    pub tasks_executed: u64,
    pub tasks_exec_time: f64,
    pub ewa_exec_time_per_task: f64,
    pub predicted_completion_time: f64,
}

impl WorkerPool {
    /// Creates a new worker pool with the number of threads equal to the `number of available CPU cores`.
    pub fn init() -> Result<Self> {
        let thread_count = std::thread::available_parallelism()?.get();
        assert!(
            thread_count >= 2,
            "worker pool must have at least 2 threads"
        );
        Self::init_with(thread_count)
    }

    /// Creates a new worker pool with the `specified number of threads`.
    pub fn init_with(n: usize) -> Result<Self> {
        Self::init_with_state(n, || Ok(()))
    }
}

impl<S: 'static> WorkerPool<S> {
    /// Creates a new worker pool, initializing state once on each worker thread.
    ///
    /// The pool calls the state initializer once inside each worker thread, and waits for it to complete.
    /// An error or panics with initialization clear dropped workers, joins the worker threads, and returns an error.
    /// State is dropped on its owning worker during shutdown.
    pub fn init_with_state<F>(n: usize, state_initializer: F) -> Result<Self>
    where
        F: Fn() -> Result<S> + Send + Sync + 'static,
    {
        assert!(n >= 2, "worker pool must have at least 2 threads");
        let initializer = Arc::new(state_initializer);

        // workers signal on this channel after all task queued are drained,
        // so that the pool shutdown can wake up when all workers are idle and close gracefully
        let (idle_tx, idle_rx) = crossbeam::channel::unbounded::<()>();

        let mut handles: Vec<WorkerHandle<S>> = Vec::with_capacity(n);
        let mut threads: Vec<JoinHandle<()>> = Vec::with_capacity(n);
        for id in 0..n {
            let initializer = Arc::clone(&initializer);
            match init_worker(id, idle_tx.clone(), move || initializer()) {
                Ok((worker, thread)) => {
                    handles.push(worker);
                    threads.push(thread);
                }
                Err(error) => {
                    drop(handles); // drop all handles, threads will be joined
                    for thread in threads {
                        let _ = thread.join();
                    }
                    return Err(error);
                }
            }
        }

        Ok(WorkerPool {
            worker_handles: handles.into_boxed_slice(),
            worker_threads: threads.into_boxed_slice(),
            idle_tx,
            idle_rx,
        })
    }

    // TODO: fn add_worker()

    // TODO: fn remove_worker()

    #[inline(always)]
    /// Submits a task directly to the worker at the given index of the worker pool, preserving its queue order.
    /// This ignores the scheduling policy and queues the task immediately to the chosen worker.
    pub fn submit_to_worker<F, T>(
        &self,
        index: usize,
        task: F,
        cost: TaskCost,
    ) -> Result<TaskHandle<T>>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.submit_to_worker_with_state(index, move |_| task(), cost)
    }

    #[inline(always)]
    /// Submits a task directly to the worker at the given index of the worker pool,
    /// similar to [`submit_to_worker`](Self::submit_to_worker) but with a state reference.
    pub fn submit_to_worker_with_state<F, T>(
        &self,
        index: usize,
        task: F,
        cost: TaskCost,
    ) -> Result<TaskHandle<T>>
    where
        F: FnOnce(&mut S) -> T + Send + 'static,
        T: Send + 'static,
    {
        let cost_value = cost.to_value();
        let worker_tx_handle = self
            .worker_handles
            .get(index)
            .ok_or_else(|| anyhow!("worker {index} is unavailable"))?;
        worker_tx_handle
            .stats
            .total_task_cost
            .fetch_add(cost_value, Ordering::Relaxed);

        let (rtx, rrx) = crossbeam::channel::bounded(1);
        let task = Task {
            cost,
            exec: exec_task!(task, rtx),
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

    // TODO: Nested parallelism does not work as of now

    /// Submits a task to the pool with `TaskCost::Normal` and returns a handle to await its result.
    ///
    /// Use [`submit_with`](Self::submit_with) for specifying a different cost and see more details.
    pub fn submit<F, T>(&self, task: F) -> Result<TaskHandle<T>>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.submit_with(task, TaskCost::Normal)
    }

    /// Submits a task to the pool with an `TaskCost`,
    /// i.e. how heavy the task is expected to be, in range of `Low` to `VeryHigh`
    /// and returns a handle for awaiting its result or errors if Pool is shut down.
    ///
    /// This uses `List Scheduling` with `Online Approximation` (instead of full scan)
    /// and `Uniform "Machine" Scheduling` to select a worker thread for executing the task.
    /// Uniform Scheduling means there scheduler accounts for the `predicted completion time` of each worker.
    /// This may `increase makespan`, but `minimizes the total busy time` of workers in most cases.
    ///
    /// > TaskCost directly affects the behavior of the scheduler and may change or be removed in the future
    pub fn submit_with<F, T>(&self, task: F, cost: TaskCost) -> Result<TaskHandle<T>>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.submit_with_state_and_cost(move |_| task(), cost)
    }

    /// Submits a task to the pool, similar to [`submit`](Self::submit), but with a state reference.
    pub fn submit_with_state<F, T>(&self, task: F) -> Result<TaskHandle<T>>
    where
        F: FnOnce(&mut S) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.submit_with_state_and_cost(task, TaskCost::Normal)
    }

    /// Submits a task to the pool, similar to [`submit_with`](Self::submit_with), but with a state reference and an explicit scheduling cost.
    pub fn submit_with_state_and_cost<F, T>(&self, task: F, cost: TaskCost) -> Result<TaskHandle<T>>
    where
        F: FnOnce(&mut S) -> T + Send + 'static,
        T: Send + 'static,
    {
        let index = {
            let n = self.worker_handles.len();
            if n == 0 {
                return Err(anyhow!("worker pool has been shut down"));
            }

            // online approximation here; 0(1) instead of 0(n) scan
            // rand sample two distinct workers and pick the one
            // with the lesser `total_tasks_cost (queued/running) * ewma_execution_time_per_task_cost`
            // i.e. find the worker that is predicted to complete it task faster
            let a = fastrand::usize(..n);
            let b = {
                let b = fastrand::usize(..n - 1);
                if b >= a { b + 1 } else { b }
            };

            // uniform machine scheduling here; basically, select worker to optimize makespan
            // the rank is a predicted time (in nanos) to drain this worker task queue
            let (wa, wb) = (&self.worker_handles[a], &self.worker_handles[b]);
            let rank_a = (wa.stats.total_task_cost.load(Ordering::Relaxed) as f64)
                .algebraic_add(cost.to_value() as f64)
                .algebraic_mul(
                    wa.stats
                        .ewma_exec_time_per_task_cost
                        .load(Ordering::Relaxed),
                );

            let rank_b = (wb.stats.total_task_cost.load(Ordering::Relaxed) as f64)
                .algebraic_add(cost.to_value() as f64)
                .algebraic_mul(
                    wb.stats
                        .ewma_exec_time_per_task_cost
                        .load(Ordering::Relaxed),
                );

            match rank_a.total_cmp(&rank_b) {
                CmpOrdering::Less => a,
                CmpOrdering::Greater => b,
                CmpOrdering::Equal => {
                    if fastrand::bool() {
                        a
                    } else {
                        b
                    }
                }
            }
        };

        self.submit_to_worker_with_state(index, task, cost)
    }

    /// Submits a batch of tasks to the pool with their respective `TaskCost`,
    /// and return the handles for awaiting their results or errors if Pool is shut down.
    ///
    /// This uses `Longest Processing Time (LPT)` scheduling, the `Uniform "Machines" Scheduling` heuristic.
    /// Uniform Scheduling means task is assigned to workers based on their completion time and overall workers state.
    /// Identical Scheduling means the scheduler care only task placement, regardless of the speed variance of workers.
    /// This aims to `optimize the makespan`, giving long tasks more room to complete faster,
    /// followed by shorter tasks, but `maximizing the total busy time` of workers.
    ///
    pub fn submit_batch<I, F, T>(&self, tasks: I) -> Result<Vec<TaskHandle<T>>>
    where
        I: Iterator<Item = (F, TaskCost)>,
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.submit_batch_with_state(tasks.map(|(task, cost)| (move |_: &mut S| task(), cost)))
    }

    /// Submits a batch of tasks to the pool, similar to [`submit_batch`](Self::submit_batch), but with a state reference.
    pub fn submit_batch_with_state<I, F, T>(&self, tasks: I) -> Result<Vec<TaskHandle<T>>>
    where
        I: Iterator<Item = (F, TaskCost)>,
        F: FnOnce(&mut S) -> T + Send + 'static,
        T: Send + 'static,
    {
        // sort descending by cost, LPT scheduling heuristic
        let mut tasks: Vec<_> = tasks.collect();
        tasks.sort_by_key(|(_, cost)| std::cmp::Reverse(cost.to_value()));

        // load all workers instant state
        let mut worker_states: Vec<(f64 /* load */, f64 /* ewma per cost */)> = self
            .worker_handles
            .iter()
            .map(|w| {
                let total_cost = w.stats.total_task_cost.load(Ordering::Relaxed) as f64;
                let ewma_per_cost = w.stats.ewma_exec_time_per_task_cost.load(Ordering::Relaxed);
                (total_cost * ewma_per_cost, ewma_per_cost)
            })
            .collect();

        // submit each task to the least loaded worker, and
        // update the load of that worker for reconciling the next task placement
        let mut handles = Vec::with_capacity(tasks.len());
        for (func, cost) in tasks {
            let cost_value = cost.to_value() as f64;

            let Some(index) = worker_states
                .iter()
                .enumerate()
                .min_by(|(_, (load_a, ewma_a)), (_, (load_b, ewma_b))| {
                    let finish_a = load_a.algebraic_add(cost_value).algebraic_mul(*ewma_a);
                    let finish_b = load_b.algebraic_add(cost_value).algebraic_mul(*ewma_b);
                    finish_a.total_cmp(&finish_b) // find the worker with the least finish time
                })
                .map(|(index, _)| index)
            else {
                return Err(anyhow!("worker pool has been shut down"));
            };

            handles.push(self.submit_to_worker_with_state(index, func, cost)?);
            let (load, ewma) = &mut worker_states[index];
            *load += cost_value * *ewma; // update the load of the selected worker
        }
        Ok(handles)
    }

    /// Blocks until the pool has `no queued tasks`, i.e. every worker has
    /// drained its queue and finished whatever it was running.
    ///
    /// Tasks that are submitted by `other threads during this call` may also be waited
    /// on, since the pool simply has to be idle. It does not prevent `submit`.
    ///
    /// > This `must not be called` from inside a task run by this pool, or it will deadlock.
    pub fn wait_all(&self) -> Result<()> {
        loop {
            if self
                .worker_handles
                .iter()
                .all(|w| w.stats.total_task_cost.load(Ordering::Relaxed) == 0)
            {
                return Ok(());
            }

            // blocks (spin -> yield -> park) until a worker reports idle
            // pool holds idle_tx, that's mean this will error only if pool is dropped (likely case)
            if let Ok(_signal) = self.idle_rx.recv() {
                // got signal here, recheck all worker again
            } else {
                return Err(anyhow!(
                    "failed to recv idle signal, possible reasons: worker thread panicked or dropped before sending the signal"
                ));
            }
        }
    }

    #[inline]
    /// Returns the total number of tasks completed by all workers in the pool.
    pub fn total_task_completed(&self) -> u64 {
        self.worker_handles
            .iter()
            .map(|w| w.stats.total_task_executed.load(Ordering::Relaxed))
            .sum()
    }

    #[inline]
    /// Returns the total number of tasks currently queued in all workers in the pool.
    pub fn total_task_queued(&self) -> u64 {
        self.worker_handles.iter().map(|w| w.tx.len() as u64).sum()
    }

    #[inline]
    /// Returns the current state for all idle/running worker in the pool.
    pub fn state(&self) -> Vec<WorkerState> {
        self.worker_handles
            .iter()
            .map(|w| WorkerState {
                tasks_queued: w.tx.len() as u64,
                tasks_cost: w.stats.total_task_cost.load(Ordering::Relaxed),
                tasks_executed: w.stats.total_task_executed.load(Ordering::Relaxed),
                tasks_exec_time: w.stats.total_task_exec_time.load(Ordering::Relaxed),
                ewa_exec_time_per_task: w
                    .stats
                    .ewma_exec_time_per_task_cost
                    .load(Ordering::Relaxed),
                predicted_completion_time: (w.stats.total_task_cost.load(Ordering::Relaxed) as f64)
                    .algebraic_mul(w.stats.ewma_exec_time_per_task_cost.load(Ordering::Relaxed)),
            })
            .collect()
    }

    // TODO: pub fn reset_state() {}

    /// Stops accepting `submit` calls, waits for all queued tasks to complete.
    ///
    /// > This `must not be called` from inside a task run by this pool, or it will hang forever.
    pub fn shutdown(&mut self) -> Result<()> {
        // drop all senders, workers will exit when their queues drain
        let worker_handles = std::mem::take(&mut self.worker_handles);
        drop(worker_handles);

        // take ownership of worker threads here
        let worker_threads = std::mem::take(&mut self.worker_threads);

        // join and check for panics
        let mut thread_panicked = 0u64;
        for threads in worker_threads {
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

impl<S: 'static> Drop for WorkerPool<S> {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
