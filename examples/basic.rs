use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use tiny_workers::{HandleOutcome, HandlesJob, Worker};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type")]
pub enum JobPayload {
    Generate(GeneratePayload),
    Resolve(ResolvePayload),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GeneratePayload {
    pub id: i64,
    pub min: i64,
    pub max: i64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResolvePayload {
    pub id: i64,
    pub sleep_ms: u64,
}

const DEFAULT_NUM_WORKERS: usize = 1;

pub struct Handler {}

#[async_trait]
impl HandlesJob<JobPayload> for Handler {
    async fn handle_job(&self, job_payload: &JobPayload) -> anyhow::Result<HandleOutcome> {
        match job_payload {
            JobPayload::Generate(_payload) => {
                println!("Generate payload.");
                tokio::time::sleep(tokio::time::Duration::from_millis(0)).await;
                Ok(HandleOutcome::Succeeded)
            }
            JobPayload::Resolve(_payload) => {
                println!("Resolve payload.");
                tokio::time::sleep(tokio::time::Duration::from_millis(0)).await;
                Ok(HandleOutcome::Retry {
                    reason: String::from("just testing"),
                })
            }
        }
    }
}

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

    // inserting some tasks

    worker.start().await?;

    Ok(())
}
