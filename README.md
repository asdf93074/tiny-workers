# tiny-workers

`tiny-workers` is a small Rust job worker library backed by SQLite.

It provides:

- A generic `Worker<T>` that processes serde-serializable job payloads
- A `HandlesJob<T>` trait for plugging in your own async job handler
- A SQLite-backed queue with leasing, retries, and terminal success/failure states
- A basic example in [`examples/basic.rs`](examples/basic.rs)

## How It Works

Jobs are stored in a `jobs` table with these states:

- `Pending`
- `Leased`
- `Succeeded`
- `Failed`

Each worker loop:

1. Claims the next available job from SQLite
2. Marks it as leased for a fixed amount of time
3. Calls your handler
4. Marks the job as succeeded, requeues it for retry, or marks it failed

The default worker configuration is:

- `max_job_attempts = 3`
- `idle_poll_interval_ms = 100`
- `lease_for_secs = 10`

## Public API

The crate re-exports the main pieces from [`src/lib.rs`](src/lib.rs):

- `Worker`
- `WorkerConfig`
- `WorkerStep`
- `JobRepo`
- `ClaimedJob`
- `JobStatus`
- `HandlesJob`
- `HandleOutcome`

## Example

The example defines a tagged enum payload and a handler implementation:

```rust
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tiny_workers::{HandleOutcome, HandlesJob, Worker};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
enum JobPayload {
    Generate { id: i64, min: i64, max: i64 },
    Resolve { id: i64, sleep_ms: u64 },
}

struct Handler;

#[async_trait]
impl HandlesJob<JobPayload> for Handler {
    async fn handle_job(&self, payload: &JobPayload) -> anyhow::Result<HandleOutcome> {
        match payload {
            JobPayload::Generate { .. } => Ok(HandleOutcome::Succeeded),
            JobPayload::Resolve { .. } => Ok(HandleOutcome::Retry {
                reason: "try again".to_string(),
            }),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let worker = Worker::new(1, Arc::new(Handler)).await?;
    worker.start().await?;
    Ok(())
}
```

## Running

Run tests:

```bash
cargo test
```

Run the example:

```bash
cargo run --example basic
```

You can optionally pass the number of worker tasks:

```bash
cargo run --example basic -- 4
```

By default, `Worker::new` opens `sqlite://local.db?mode=rwc`, so running the worker creates or reuses `local.db` in the project directory.

## Notes

- The crate package name is `tiny-workers` in `Cargo.toml`.
- The example starts the worker loop but does not enqueue sample jobs yet.
- `Worker::start()` runs indefinitely and is intended for long-lived worker processes.
