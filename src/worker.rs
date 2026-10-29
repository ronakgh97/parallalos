use crate::Task;
use anyhow::{Result, anyhow};
use crossbeam::channel::Sender;
use portable_atomic::AtomicF64;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::Instant;

/// Handle to a worker thread, recv tasks from scheduler
/// and run them until the scheduler is dropped
pub struct WorkerHandle<S = ()> {
    #[allow(unused)]
    pub id: usize,
    pub tx: Sender<Task<S>>,
    pub stats: Arc<WorkerStats>,
}

/// Initialize worker thread `id` and return its `WorkerHandle` and `thread's JoinHandle`
///
/// This spawn thread and run tasks until every `WorkerHandle` is dropped.
/// According to `crossbeam docs`, this will return only if `all senders are dropped` AND `the channel is empty`,
/// therefore this drains the channel even if schedular has drop tx_handles, and return on empty channel.
///
/// Sends one tick on `idle_tx` after completing all task queued to it,
/// so that the pool can wait for `all workers to be idle` and shutdown gracefully.
pub fn init_worker<S, F>(
    id: usize,
    idle_tx: Sender<()>,
    initializer: F,
) -> Result<(WorkerHandle<S>, JoinHandle<()>)>
where
    S: 'static,
    F: FnOnce() -> Result<S> + Send + 'static,
{
    let (tx, rx) = crossbeam::channel::unbounded::<Task<S>>();
    let (ready_tx, ready_rx) = crossbeam::channel::bounded(1);
    let stats = Arc::new(WorkerStats {
        total_task_cost: AtomicU64::new(0),
        total_task_executed: AtomicU64::new(0),
        total_task_exec_time: AtomicF64::new(0.0),
        ewma_exec_time_per_task_cost: AtomicF64::new(22.5), // zero would cause weird behavior on cold start worker
    });
    let thread_stats = Arc::clone(&stats);
    let thread_handle = std::thread::Builder::new()
        .name(format!("worker-{id}"))
        .spawn(move || {
            // initialize state
            let mut state = match catch_unwind(AssertUnwindSafe(initializer)) {
                Ok(Ok(state)) => state,
                Ok(Err(error)) => {
                    let _ = ready_tx.send(Err(
                        error.context(format!("worker {id} initialization failed"))
                    ));
                    return;
                }
                Err(_) => {
                    let _ = ready_tx.send(Err(anyhow!("worker {id} initializer panicked")));
                    return;
                }
            };
            if ready_tx.send(Ok(())).is_err() {
                return;
            }
            while let Ok(task) = rx.recv() {
                let start = Instant::now();
                // execute the task, set id and state by the worker executing it
                // and return value to TaskHandle via oneshot channel
                (task.exec)(&mut state, id);
                let elapsed = start.elapsed().as_nanos() as f64;
                let cost = task.cost.to_value();
                let drained = thread_stats
                    .total_task_cost
                    .fetch_sub(cost, Ordering::Relaxed)
                    == cost; // this becomes true after executing the last task in the queue
                thread_stats
                    .total_task_exec_time
                    .fetch_add(elapsed, Ordering::Relaxed);
                thread_stats.update_ewa(elapsed, cost);
                thread_stats
                    .total_task_executed
                    .fetch_add(1, Ordering::Relaxed);

                // signal the pool that this worker has drained
                // its queue and is idle at this instant
                if drained {
                    let _ = idle_tx.send(());
                }
            }
        })?;
    let initialized = ready_rx
        .recv()
        .map_err(|_| anyhow!("worker {id} exited during initialization"))
        .and_then(|result| result);
    if let Err(error) = initialized {
        drop(tx);
        let _ = thread_handle.join();
        return Err(error);
    }
    Ok((WorkerHandle { id, tx, stats }, thread_handle))
}

/// Stats of worker, needed for scheduling tasks to suitable worker thread
pub struct WorkerStats {
    pub total_task_cost: AtomicU64,
    pub total_task_executed: AtomicU64,
    pub total_task_exec_time: AtomicF64,
    pub ewma_exec_time_per_task_cost: AtomicF64,
}

impl WorkerStats {
    /// Update the exponentially weighted `moving average` of execution time
    /// per cost unit, i.e. observed nanos per unit of declared `TaskCost` weight.
    #[inline(always)]
    fn update_ewa(&self, elapsed: f64, cost: u64) {
        // formula; new_ewa = observed variable * alpha + old_ewa * (1 - alpha)
        const ALPHA: f64 = 1.0 / 8.0;
        let variable = elapsed.algebraic_div(cost as f64);
        let current = self.ewma_exec_time_per_task_cost.load(Ordering::Relaxed);
        let new_ewa = ALPHA.algebraic_mul(variable) + (1.0 - ALPHA).algebraic_mul(current);
        self.ewma_exec_time_per_task_cost
            .store(new_ewa, Ordering::Relaxed);
    }
}
