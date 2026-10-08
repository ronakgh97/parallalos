use crate::tasks::TaskCost;

pub mod pool;
pub mod tasks;
pub(crate) mod worker;

/// A unit of work to be executed by a worker thread
pub(crate) struct Task {
    /// Enum that denotes how "costly" the task is
    pub cost: TaskCost,
    /// Closure that capture callable and worker id that's executing it
    pub exec: Box<dyn FnOnce(usize) + Send + 'static>,
}

#[test]
fn lib_test() {
    let pool = pool::WorkerPool::init_with(4).unwrap();
    let counter = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));

    let task_handles: Vec<_> = (0..100)
        .map(|_| {
            let counter_clone = std::sync::Arc::clone(&counter);
            pool.submit(move || {
                counter_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            })
        })
        .collect();

    for handle in task_handles {
        let _ = handle.unwrap().wait();
    }

    // pool.wait_all_tasks();

    assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 100);
}
