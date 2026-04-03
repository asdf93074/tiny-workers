use std::{str::FromStr, sync::Arc};

use sqlx::{SqlitePool, sqlite::SqliteConnectOptions};

use crate::{HandleOutcome, HandlesJob, JobRepo, utils::unix_now};

#[derive(Debug, PartialEq, Eq)]
pub enum WorkerStep {
    Idle,
    Succeeded { job_id: i64 },
    Retry { job_id: i64, reason: String },
    Failure { job_id: i64, reason: String },
}

const MAX_JOB_ATTEMPTS: i32 = 3;

async fn run_worker_once(
    worker_id: usize,
    repo: &JobRepo,
    handler: &dyn HandlesJob,
) -> anyhow::Result<WorkerStep> {
    let maybe_job = repo.claim_next(unix_now(), 10).await?;

    let Some(job) = maybe_job else {
        println!("[{worker_id}] No jobs available.");
        return Ok(WorkerStep::Idle);
    };

    let outcome = handler.handle_job(&job.payload).await;
    let job_id = job.queue_id;

    match outcome {
        Ok(HandleOutcome::Succeeded) => {
            println!("[{worker_id}] Job {job_id} finished successfully.");
            repo.mark_succeeded(job_id).await?;
            Ok(WorkerStep::Succeeded { job_id })
        }
        Ok(HandleOutcome::Retry { reason }) => {
            if job.attempts == MAX_JOB_ATTEMPTS {
                println!(
                    "[{worker_id}] Job {job_id} failed 3 times. Reason: {reason}. Marking as failed."
                );
                repo.mark_failed(job_id).await?;
                Ok(WorkerStep::Failure { job_id, reason })
            } else {
                println!("[{worker_id}] Job {job_id} failed. Reason: {reason}. Requeuing.");
                repo.requeue(job_id, job.attempts, &reason).await?;
                Ok(WorkerStep::Retry { job_id, reason })
            }
        }
        Err(e) => {
            eprintln!("[{worker_id}] Failed to process job {job_id}. {e}");
            repo.mark_failed(job_id).await?;
            Ok(WorkerStep::Failure {
                job_id,
                reason: e.to_string(),
            })
        }
    }
}

async fn run_worker(
    worker_id: usize,
    repo: Arc<JobRepo>,
    handler: Arc<dyn HandlesJob>,
) -> anyhow::Result<()> {
    loop {
        match run_worker_once(worker_id, &repo, &*handler).await {
            Ok(WorkerStep::Idle) => {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await
            }
            _ => {}
        }
    }
}

pub struct Worker {
    num_workers: usize,
    repo: Arc<JobRepo>,
    handler: Arc<dyn HandlesJob>,
}

impl Worker {
    pub async fn new(num_workers: usize, handler: Arc<dyn HandlesJob>) -> anyhow::Result<Self> {
        let opts = SqliteConnectOptions::from_str("sqlite://local.db?mode=rwc")?
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
        let repo = Arc::new(JobRepo {
            pool: SqlitePool::connect_with(opts).await.unwrap(),
        });

        repo.create_tables().await?;

        Ok(Self {
            num_workers,
            repo,
            handler,
        })
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        let mut worker_handles = Vec::new();
        let handler = &self.handler;

        println!("Starting with {} workers.", self.num_workers);
        for i in 0..self.num_workers {
            worker_handles.push(tokio::spawn(run_worker(
                i,
                self.repo.clone(),
                handler.clone(),
            )));
        }

        for h in worker_handles {
            let _ = h.await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{Handler, JobPayload};

    use crate::handlers::mock::*;

    async fn setup() -> anyhow::Result<Arc<JobRepo>> {
        let opts = SqliteConnectOptions::from_str("sqlite::memory:")?
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
        let repo = Arc::new(JobRepo {
            pool: SqlitePool::connect_with(opts).await.unwrap(),
        });

        repo.create_tables().await?;

        Ok(repo)
    }

    #[tokio::test]
    async fn enqueue_then_claim_succeeds() -> anyhow::Result<()> {
        let repo = setup().await.unwrap();
        let job_payload = JobPayload::Generate(crate::GeneratePayload {
            id: 1,
            min: 10,
            max: 20,
        });
        assert_eq!(repo.enqueue(&job_payload).await.unwrap(), 1);

        let repo_clone = repo.clone();
        let handler = Arc::new(AlwaysSucceedHandler {});

        let t = tokio::spawn(async move {
            let res = run_worker_once(0, &repo_clone, &*handler).await;
            assert_eq!(WorkerStep::Succeeded { job_id: 1 }, res.unwrap());
        });

        t.await?;

        Ok(())
    }

    #[tokio::test]
    async fn retry_works() -> anyhow::Result<()> {
        let repo = setup().await.unwrap();
        let job_payload = JobPayload::Generate(crate::GeneratePayload {
            id: 1,
            min: 10,
            max: 20,
        });
        assert_eq!(repo.enqueue(&job_payload).await.unwrap(), 1);

        let repo_clone = repo.clone();
        let handler = Arc::new(AlwaysRetryHandler {});

        let t = tokio::spawn(async move {
            let res = run_worker_once(0, &repo_clone, &*handler).await;
            assert_eq!(
                WorkerStep::Retry {
                    job_id: 1,
                    reason: "alwaysretries".to_string()
                },
                res.unwrap()
            );
        });

        t.await?;

        Ok(())
    }

    #[tokio::test]
    async fn works_after_one_retry() -> anyhow::Result<()> {
        let repo = setup().await.unwrap();
        let job_payload = JobPayload::Generate(crate::GeneratePayload {
            id: 1,
            min: 10,
            max: 20,
        });
        assert_eq!(repo.enqueue(&job_payload).await.unwrap(), 1);

        let repo_clone = repo.clone();
        let retry_handler = Arc::new(AlwaysRetryHandler {});
        let success_handler = Arc::new(AlwaysSucceedHandler {});

        let t = tokio::spawn(async move {
            let res = run_worker_once(0, &repo_clone, &*retry_handler).await;
            assert_eq!(
                res.unwrap(),
                WorkerStep::Retry {
                    job_id: 1,
                    reason: "alwaysretries".to_string()
                },
            );
            let res = run_worker_once(0, &repo_clone, &*success_handler).await;
            assert_eq!(res.unwrap(), WorkerStep::Succeeded { job_id: 1 },);
        });

        t.await?;

        Ok(())
    }

    #[tokio::test]
    async fn three_retries_and_failure() -> anyhow::Result<()> {
        let repo = setup().await.unwrap();
        let job_payload = JobPayload::Generate(crate::GeneratePayload {
            id: 1,
            min: 10,
            max: 20,
        });
        assert_eq!(repo.enqueue(&job_payload).await.unwrap(), 1);

        let repo_clone = repo.clone();
        let handler = Arc::new(AlwaysRetryHandler {});

        let t = tokio::spawn(async move {
            let res = run_worker_once(0, &repo_clone, &*handler).await;
            assert_eq!(
                res.unwrap(),
                WorkerStep::Retry {
                    job_id: 1,
                    reason: "alwaysretries".to_string()
                },
            );
            let res = run_worker_once(0, &repo_clone, &*handler).await;
            assert_eq!(
                res.unwrap(),
                WorkerStep::Retry {
                    job_id: 1,
                    reason: "alwaysretries".to_string()
                },
            );
            let res = run_worker_once(0, &repo_clone, &*handler).await;
            assert_eq!(
                res.unwrap(),
                WorkerStep::Failure {
                    job_id: 1,
                    reason: "alwaysretries".to_string()
                },
            );
        });

        t.await?;

        Ok(())
    }

    #[tokio::test]
    async fn failure_works() -> anyhow::Result<()> {
        let repo = setup().await.unwrap();
        let job_payload = JobPayload::Generate(crate::GeneratePayload {
            id: 1,
            min: 10,
            max: 20,
        });
        assert_eq!(repo.enqueue(&job_payload).await.unwrap(), 1);

        let repo_clone = repo.clone();
        let handler = Arc::new(AlwaysFailsHandler {});

        let t = tokio::spawn(async move {
            let res = run_worker_once(0, &repo_clone, &*handler).await;
            assert_eq!(
                WorkerStep::Failure {
                    job_id: 1,
                    reason: "alwaysfails".to_string()
                },
                res.unwrap()
            );
        });

        t.await?;

        Ok(())
    }

    #[tokio::test]
    async fn only_one_claims_when_two_workers() -> anyhow::Result<()> {
        let repo = setup().await.unwrap();
        let job_payload = JobPayload::Generate(crate::GeneratePayload {
            id: 1,
            min: 10,
            max: 20,
        });
        assert_eq!(repo.enqueue(&job_payload).await.unwrap(), 1);

        let repo_clone_1 = repo.clone();
        let repo_clone_2 = repo.clone();
        let handler_1 = Arc::new(Handler {});
        let handler_2 = Arc::new(Handler {});

        let t1 = tokio::spawn(async move { run_worker_once(0, &repo_clone_1, &*handler_1).await });

        let t2 = tokio::spawn(async move { run_worker_once(1, &repo_clone_2, &*handler_2).await });

        let res1 = t1.await.unwrap()?;
        let res2 = t2.await.unwrap()?;

        assert!(matches!(
            (res1, res2),
            (WorkerStep::Idle, WorkerStep::Succeeded { job_id: 1 })
                | (WorkerStep::Succeeded { job_id: 1 }, WorkerStep::Idle),
        ));

        Ok(())
    }
}
