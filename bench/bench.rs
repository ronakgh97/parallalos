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
    for _ in 0..(1 << 20) {
        p.extend_from_slice(
            format!(
                "user=u{:05} action=login ip=10.{}.{}.{} code={:?}\n",
                fastrand::u64(..100_000),
                fastrand::u8(..255),
                fastrand::u8(..255),
                fastrand::u8(..255),
                fastrand::choice(["200", "201", "202", "400", "401", "403", "500"])
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
            TaskKind::Hex => TaskCost::Low,
            TaskKind::Sha => TaskCost::Normal,
            TaskKind::Aes => TaskCost::Moderate,
            TaskKind::Zstd => TaskCost::High,
        }
    }
    fn run(self) {
        match self {
            TaskKind::Hex => {
                black_box(hex::encode(SHARED_PAYLOAD.as_ref()));
            }
            TaskKind::Sha => {
                black_box(sha2::Sha256::digest(SHARED_PAYLOAD.as_ref()));
            }
            TaskKind::Aes => {
                let nonce = Nonce::generate();
                black_box(&SHARED_AES_KEY.encrypt(&nonce, SHARED_PAYLOAD.as_ref()).ok());
            }
            TaskKind::Zstd => {
                black_box(zstd::bulk::compress(SHARED_PAYLOAD.as_ref(), 8).ok());
            }
        }
    }
}

const WORKERS: usize = 16; // I have intel i7-14650hx, 24 Cores
const BATCH: usize = 4096;

fn main() -> Result<()> {
    let wp = WorkerPool::init_with(WORKERS)?;
    println!(
        "{WORKERS} workers, {BATCH} tasks, payload {} MB\n",
        SHARED_PAYLOAD.len() / (1 << 20)
    );

    // TODO; Add warmup
    let start = Instant::now();
    let mut handles = Vec::with_capacity(BATCH);
    for _ in 0..BATCH {
        let task = TaskKind::ALL[fastrand::usize(..TaskKind::ALL.len())];
        handles.push(wp.submit_with_cost(move || task.run(), task.cost())?);
    }
    for h in handles {
        h.wait()?;
    }
    let makespan = start.elapsed().as_secs_f64();

    let worker_states = wp.stats();
    let total_worker_busy_time =
        worker_states.iter().map(|w| w.tasks_exec_time).sum::<u64>() as f64;

    println!(
        "{:>5} {:>7} {:>11} {:>8}",
        "worker", "tasks", "busy(s)", "utils%"
    );
    println!("{}", "-".repeat(32));
    for (i, w) in worker_states.iter().enumerate() {
        println!(
            "{:>5} {:>7} {:>11.3} {:>8.3}%",
            i,
            w.tasks_executed,
            w.tasks_exec_time as f64 / 1_000_000_000.0,
            w.tasks_exec_time as f64 / (makespan * 1_000_000_000.0) * 100.0,
        );
    }
    println!("{}", "-".repeat(32));
    println!(
        "makespan: {:.3} sec\ntotal busy: {:.3} sec\ntotal utils: {:.2}%",
        makespan,
        total_worker_busy_time / 1_000_000_000.0,
        total_worker_busy_time / (makespan * WORKERS as f64 * 1_000_000_000.0) * 100.0
    );

    Ok(())
}
