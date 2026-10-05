pub mod core;
pub mod tasks;
pub(crate) mod worker;

/// A unit of work to be executed by a worker thread.
pub(crate) struct Task {
    pub cost: u64,
    pub exec: Box<dyn FnOnce() + Send + 'static>,
}

#[test]
fn test() {
    let pool = core::Pool::init_with(4).unwrap();
    let counter = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));

    let _task_handles: Vec<_> = (0..100)
        .map(|_| {
            let counter_clone = std::sync::Arc::clone(&counter);
            pool.submit(move || {
                counter_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            })
        })
        .collect();

    // for handle in task_handles {
    //     let _ = handle.unwrap().wait();
    // }

    pool.wait_all_tasks();

    assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 100);
}
