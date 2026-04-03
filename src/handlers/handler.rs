use async_trait::async_trait;

use crate::JobPayload;

pub enum HandleOutcome {
    Succeeded,
    Retry { reason: String },
}

pub struct Handler {}

#[async_trait]
pub trait HandlesJob: Send + Sync {
    async fn handle_job(&self, job_payload: &JobPayload) -> anyhow::Result<HandleOutcome>;
}

#[async_trait]
impl HandlesJob for Handler {
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
