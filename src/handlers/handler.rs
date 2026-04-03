use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};

pub enum HandleOutcome {
    Succeeded,
    Retry { reason: String },
}

#[async_trait]
pub trait HandlesJob<T: Serialize + DeserializeOwned>: Send + Sync {
    async fn handle_job(&self, job_payload: &T) -> anyhow::Result<HandleOutcome>;
}

#[cfg(test)]
pub mod test {
    use async_trait::async_trait;

    use crate::jobs::test::JobPayload;

    use super::*;

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

    pub struct AlwaysSucceedHandler {}

    #[async_trait]
    impl HandlesJob<JobPayload> for AlwaysSucceedHandler {
        async fn handle_job(&self, _job_payload: &JobPayload) -> anyhow::Result<HandleOutcome> {
            Ok(HandleOutcome::Succeeded)
        }
    }

    pub struct AlwaysRetryHandler {}

    #[async_trait]
    impl HandlesJob<JobPayload> for AlwaysRetryHandler {
        async fn handle_job(&self, _job_payload: &JobPayload) -> anyhow::Result<HandleOutcome> {
            Ok(HandleOutcome::Retry {
                reason: "alwaysretries".to_string(),
            })
        }
    }

    pub struct AlwaysFailsHandler {}

    #[async_trait]
    impl HandlesJob<JobPayload> for AlwaysFailsHandler {
        async fn handle_job(&self, _job_payload: &JobPayload) -> anyhow::Result<HandleOutcome> {
            Err(anyhow::anyhow!("alwaysfails"))
        }
    }
}
