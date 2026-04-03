use std::sync::Arc;

use workerv2::{Handler, Worker};

const DEFAULT_NUM_WORKERS: usize = 1;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut num_workers: usize = DEFAULT_NUM_WORKERS;
    let args: Vec<String> = std::env::args().collect();
    for a in &args[1..] {
        match a.parse() {
            Ok(i) => num_workers = i,
            Err(_) => {
                eprintln!("Invalid number of workers passed: {a}.");
                eprintln!("Defaulting to 1");
            }
        }
    }

    let worker = Worker::new(num_workers, Arc::new(Handler {})).await?;

    worker.start().await?;

    Ok(())
}
