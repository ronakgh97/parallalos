pub mod core;
pub(crate) mod jobs;
pub(crate) mod worker;

/// Job is a boxed closure that can be `executed once`, can be `sent across threads`, and has a `static lifetime`.
pub(crate) type Job = Box<dyn FnOnce() + Send + 'static>;
