pub mod core;
pub(crate) mod tasks;
pub(crate) mod worker;

/// A unit of work to be executed by a worker thread.
pub(crate) struct Task {
    pub cost: u64,
    pub exec: Box<dyn FnOnce() + Send + 'static>,
}
