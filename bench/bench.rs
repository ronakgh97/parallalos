use aes_gcm::Nonce;
use aes_gcm::aead::{Aead, Generate, Key, KeyInit};
use anyhow::Result;
use parallelos::pool::WorkerPool;
use parallelos::tasks::TaskCost;
use sha2::Digest;
use std::hint::black_box;
use std::sync::LazyLock;
use std::time::Instant;

static SHARED_AES_KEY: LazyLock<aes_gcm::Aes256Gcm> = LazyLock::new(|| {
    let key_material = Key::<aes_gcm::Aes256Gcm>::generate();
    aes_gcm::Aes256Gcm::new(&key_material)
});

static SHARED_PAYLOAD: LazyLock<Box<[u8]>> = LazyLock::new(|| {
    let mut p = vec![];
    for _ in 0..448667 {
        p.extend_from_slice(
            format!(
                "user=u{:05}, action={:?}, port={}, ip=10.{}.{}.{}, status_code={}\n",
                fastrand::u64(..100_000),
                fastrand::choice(["login", "logout", "view"]),
                fastrand::u16(..65535),
                fastrand::u8(..255),
                fastrand::u8(..255),
                fastrand::u8(..255),
                fastrand::u64(400..429)
            )
            .as_bytes(),
        );
    }
    p.into_boxed_slice()
});

#[derive(Clone, Copy)]
enum TaskKind {
    Hex,
    Sha,
    Aes,
    Zstd,
}

impl TaskKind {
    const ALL: [TaskKind; 4] = [TaskKind::Hex, TaskKind::Sha, TaskKind::Aes, TaskKind::Zstd];
    fn cost(self) -> TaskCost {
        match self {
            TaskKind::Hex => TaskCost::Normal,
            TaskKind::Sha => TaskCost::Low,
            TaskKind::Aes => TaskCost::Low,
            TaskKind::Zstd => TaskCost::High,
        }
    }
    fn run(self) -> u64 {
        match self {
            TaskKind::Hex => {
                let start = Instant::now();
                black_box(hex::encode(SHARED_PAYLOAD.as_ref()));
                start.elapsed().as_nanos() as u64
            }
            TaskKind::Sha => {
                let start = Instant::now();
                black_box(sha2::Sha256::digest(SHARED_PAYLOAD.as_ref()));
                start.elapsed().as_nanos() as u64
            }
            TaskKind::Aes => {
                let start = Instant::now();
                let nonce = Nonce::generate();
                black_box(&SHARED_AES_KEY.encrypt(&nonce, SHARED_PAYLOAD.as_ref()).ok());
                start.elapsed().as_nanos() as u64
            }
            TaskKind::Zstd => {
                let start = Instant::now();
                black_box(zstd::bulk::compress(SHARED_PAYLOAD.as_ref(), 8).ok());
                start.elapsed().as_nanos() as u64
            }
        }
    }
}

const WORKERS: usize = 16; // I have intel i7-14650hx, 24 Cores
const TASKS: usize = 4168;
const WARMUP_TASKS: usize = 1536;

fn main() -> Result<()> {
    use indicatif::{ProgressBar, ProgressStyle};
    {
        let pb_task = ProgressBar::new(WARMUP_TASKS as u64);
        println!("warming up with {WARMUP_TASKS} tasks");
        pb_task.set_style(ProgressStyle::with_template(
            " {bar:40.cyan/blue} {pos:3}/{len:3} task",
        )?);

        let pool = WorkerPool::init_with(WORKERS + 2)?;
        for _ in 0..WARMUP_TASKS {
            let task = TaskKind::ALL[fastrand::usize(..TaskKind::ALL.len())];
            pool.submit_with(move || task.run(), task.cost())?;
        }

        loop {
            let total_task_submitted = pool.total_task_completed();
            pb_task.set_position(total_task_submitted);
            if total_task_submitted >= WARMUP_TASKS as u64 {
                break;
            }
            std::thread::yield_now();
            // std::thread::sleep(std::time::Duration::from_millis(50));
        }
        pb_task.finish_and_clear();
        black_box(pool.wait_all()?);
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    let pool = WorkerPool::init_with(WORKERS)?;
    let pb_task = ProgressBar::new(TASKS as u64);
    let mut hex_task_handles = Vec::with_capacity(TASKS);
    let mut sha_task_handles = Vec::with_capacity(TASKS);
    let mut aes_task_handles = Vec::with_capacity(TASKS);
    let mut zstd_task_handles = Vec::with_capacity(TASKS);
    println!(
        "running {TASKS} tasks with {WORKERS} workers (payload {} MB)\n",
        SHARED_PAYLOAD.len() / (1 << 20)
    );
    pb_task.set_style(ProgressStyle::with_template(
        " {bar:40.cyan/blue} {pos:3}/{len:3} task",
    )?);

    let start = Instant::now();
    for _ in 0..TASKS {
        let task = TaskKind::ALL[fastrand::usize(..TaskKind::ALL.len())];
        match task {
            TaskKind::Hex => {
                hex_task_handles.push(pool.submit_with(move || task.run(), task.cost())?)
            }
            TaskKind::Sha => {
                sha_task_handles.push(pool.submit_with(move || task.run(), task.cost())?)
            }
            TaskKind::Aes => {
                aes_task_handles.push(pool.submit_with(move || task.run(), task.cost())?)
            }
            TaskKind::Zstd => {
                zstd_task_handles.push(pool.submit_with(move || task.run(), task.cost())?)
            }
        }
    }

    loop {
        let total_task_submitted = pool.total_task_completed();
        pb_task.set_position(total_task_submitted);
        if total_task_submitted >= TASKS as u64 {
            break;
        }
        std::thread::yield_now();
        // std::thread::sleep(std::time::Duration::from_millis(50));
    }
    pb_task.finish_and_clear();
    black_box(pool.wait_all()?);

    let makespan = start.elapsed().as_secs_f64();
    let worker_states = pool.state();

    // wait_all above make sure, that these unwrap will not panic
    let hex_task_time = hex_task_handles
        .into_iter()
        .map(|h| h.wait().unwrap().0)
        .sum::<u64>() as f64;
    let sha_task_time = sha_task_handles
        .into_iter()
        .map(|h| h.wait().unwrap().0)
        .sum::<u64>() as f64;
    let aes_task_time = aes_task_handles
        .into_iter()
        .map(|h| h.wait().unwrap().0)
        .sum::<u64>() as f64;
    let zstd_task_time = zstd_task_handles
        .into_iter()
        .map(|h| h.wait().unwrap().0)
        .sum::<u64>() as f64;

    println!(
        "{:>5} {:>7} {:>11} {:>8}",
        "worker", "tasks", "busy", "util"
    );
    println!("{}", "-".repeat(36));
    for (i, w) in worker_states.iter().enumerate() {
        println!(
            "{:>5} {:>7} {:>11.2}s {:>8.2}%",
            i,
            w.tasks_executed,
            w.tasks_exec_time / 1_000_000_000.0,
            w.tasks_exec_time / (makespan * 1_000_000_000.0) * 100.0,
        );
    }
    println!("{}", "-".repeat(36));

    let total_worker_busy_time = worker_states.iter().map(|w| w.tasks_exec_time).sum::<f64>();
    println!(
        "hex: {:.3} sec ({:.2}%) \nsha: {:.3} sec ({:.2}%) \naes: {:.3} sec ({:.2}%) \nzstd: {:.3} sec ({:.2}%)",
        hex_task_time / 1_000_000_000.0,
        hex_task_time / (total_worker_busy_time) * 100.0,
        sha_task_time / 1_000_000_000.0,
        sha_task_time / (total_worker_busy_time) * 100.0,
        aes_task_time / 1_000_000_000.0,
        aes_task_time / (total_worker_busy_time) * 100.0,
        zstd_task_time / 1_000_000_000.0,
        zstd_task_time / (total_worker_busy_time) * 100.0
    );

    println!();

    println!(
        "achieved makespan: {:.3} sec\nideal makespan: {:.3} sec\ntotal busy: {:.3} sec\ntotal util: {:.2}%",
        makespan,
        total_worker_busy_time / 1_000_000_000.0 / WORKERS as f64,
        total_worker_busy_time / 1_000_000_000.0,
        total_worker_busy_time / (makespan * WORKERS as f64 * 1_000_000_000.0) * 100.0
    );

    Ok(())
}
