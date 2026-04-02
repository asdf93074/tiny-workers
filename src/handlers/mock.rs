use async_trait::async_trait;

use crate::JobPayload;
use crate::handler::{HandleOutcome, HandlesJob};

pub struct AlwaysSucceedHandler {}

#[async_trait]
impl HandlesJob for AlwaysSucceedHandler {
    async fn handle_job(&self, _job_payload: &JobPayload) -> anyhow::Result<HandleOutcome> {
        Ok(HandleOutcome::Succeeded)
    }
}

pub struct AlwaysRetryHandler {}

#[async_trait]
impl HandlesJob for AlwaysRetryHandler {
    async fn handle_job(&self, _job_payload: &JobPayload) -> anyhow::Result<HandleOutcome> {
        Ok(HandleOutcome::Retry {
            reason: "alwaysretries".to_string(),
        })
    }
}

pub struct AlwaysFailsHandler {}

#[async_trait]
impl HandlesJob for AlwaysFailsHandler {
    async fn handle_job(&self, _job_payload: &JobPayload) -> anyhow::Result<HandleOutcome> {
        Err(anyhow::anyhow!("alwaysfails"))
    }
}
