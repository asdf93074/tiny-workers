use std::{marker::PhantomData, str::FromStr, sync::Arc};

use serde::{Serialize, de::DeserializeOwned};
use sqlx::{SqlitePool, sqlite::SqliteConnectOptions};

use crate::{HandleOutcome, HandlesJob, JobRepo, utils::unix_now};

#[derive(Debug, PartialEq, Eq)]
pub enum WorkerStep {
    Idle,
    Succeeded { job_id: i64 },
    Retry { job_id: i64, reason: String },
    Failure { job_id: i64, reason: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerConfig {
    pub max_job_attempts: i32,
    pub idle_poll_interval_ms: u64,
    pub lease_for_secs: i64,
}

pub struct Worker<T> {
    pub num_workers: usize,
    pub repo: Arc<JobRepo<T>>,
    handler: Arc<dyn HandlesJob<T>>,
    pub config: WorkerConfig,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            max_job_attempts: 3,
            idle_poll_interval_ms: 100,
            lease_for_secs: 10,
        }
    }
}

impl<T> Worker<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub async fn new(num_workers: usize, handler: Arc<dyn HandlesJob<T>>) -> anyhow::Result<Self> {
        let opts = SqliteConnectOptions::from_str("sqlite://local.db?mode=rwc")?
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
        let repo = Arc::new(JobRepo::<T> {
            pool: SqlitePool::connect_with(opts).await.unwrap(),
            _marker: PhantomData,
        });

        repo.create_tables().await?;

        Ok(Self {
            num_workers,
            repo,
            handler,
            config: Default::default(),
        })
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        let mut worker_handles = Vec::new();

        println!("Starting with {} workers.", self.num_workers);
        for i in 0..self.num_workers {
            let repo = self.repo.clone();
            let handler = self.handler.clone();
            let config = self.config.clone();
            worker_handles.push(tokio::spawn(async move {
                Self::run_worker(i, repo, handler, config).await
            }));
        }

        for h in worker_handles {
            let _ = h.await?;
        }

        Ok(())
    }

    async fn run_worker_once(
        worker_id: usize,
        repo: &JobRepo<T>,
        handler: &dyn HandlesJob<T>,
        config: &WorkerConfig,
    ) -> anyhow::Result<WorkerStep> {
        let maybe_job = repo.claim_next(unix_now(), config.lease_for_secs).await?;

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
                if job.attempts == config.max_job_attempts {
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
        repo: Arc<JobRepo<T>>,
        handler: Arc<dyn HandlesJob<T>>,
        config: WorkerConfig,
    ) -> anyhow::Result<()> {
        loop {
            match Self::run_worker_once(worker_id, &repo, &*handler, &config).await {
                Ok(WorkerStep::Idle) => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(config.idle_poll_interval_ms)).await
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::handlers::handler::test::*;
    use crate::jobs::test::{GeneratePayload, JobPayload};

    async fn setup() -> anyhow::Result<Arc<JobRepo<JobPayload>>> {
        let opts = SqliteConnectOptions::from_str("sqlite::memory:")?
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
        let repo = Arc::new(JobRepo::<JobPayload> {
            pool: SqlitePool::connect_with(opts).await.unwrap(),
            _marker: PhantomData,
        });

        repo.create_tables().await?;

        Ok(repo)
    }

    #[tokio::test]
    async fn enqueue_then_claim_succeeds() -> anyhow::Result<()> {
        let handler = Arc::new(AlwaysSucceedHandler {});
        let worker = Worker {
            num_workers: 1,
            repo: setup().await?,
            handler,
            config: Default::default(),
        };
        let job_payload = JobPayload::Generate(GeneratePayload {
            id: 1,
            min: 10,
            max: 20,
        });
        assert_eq!(worker.repo.enqueue(&job_payload).await.unwrap(), 1);

        let repo = worker.repo.clone();
        let handler = worker.handler.clone();
        let config = worker.config.clone();
        let t = tokio::spawn(async move {
            let res = Worker::run_worker_once(0, &repo, &*handler, &config).await;
            assert_eq!(WorkerStep::Succeeded { job_id: 1 }, res.unwrap());
        });

        t.await?;

        Ok(())
    }

    #[tokio::test]
    async fn retry_works() -> anyhow::Result<()> {
        let handler = Arc::new(AlwaysRetryHandler {});
        let worker = Worker {
            num_workers: 1,
            repo: setup().await?,
            handler,
            config: Default::default(),
        };
        let job_payload = JobPayload::Generate(GeneratePayload {
            id: 1,
            min: 10,
            max: 20,
        });
        assert_eq!(worker.repo.enqueue(&job_payload).await.unwrap(), 1);

        let repo = worker.repo.clone();
        let handler = worker.handler.clone();
        let config = worker.config.clone();
        let t = tokio::spawn(async move {
            let res = Worker::run_worker_once(0, &repo, &*handler, &config).await;
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
        let retry_handler = Arc::new(AlwaysRetryHandler {});
        let mut worker = Worker {
            num_workers: 1,
            repo: setup().await?,
            handler: retry_handler,
            config: Default::default(),
        };
        let success_handler = Arc::new(AlwaysSucceedHandler {});
        let job_payload = JobPayload::Generate(GeneratePayload {
            id: 1,
            min: 10,
            max: 20,
        });
        assert_eq!(worker.repo.enqueue(&job_payload).await.unwrap(), 1);

        let t = tokio::spawn(async move {
            let res =
                Worker::run_worker_once(0, &worker.repo, &*worker.handler, &worker.config).await;
            assert_eq!(
                res.unwrap(),
                WorkerStep::Retry {
                    job_id: 1,
                    reason: "alwaysretries".to_string()
                },
            );
            worker.handler = success_handler.clone();
            let res =
                Worker::run_worker_once(0, &worker.repo, &*worker.handler, &worker.config).await;
            assert_eq!(res.unwrap(), WorkerStep::Succeeded { job_id: 1 },);
        });

        t.await?;

        Ok(())
    }

    #[tokio::test]
    async fn three_retries_and_failure() -> anyhow::Result<()> {
        let handler = Arc::new(AlwaysRetryHandler {});
        let worker = Worker {
            num_workers: 1,
            repo: setup().await?,
            handler,
            config: Default::default(),
        };
        let job_payload = JobPayload::Generate(GeneratePayload {
            id: 1,
            min: 10,
            max: 20,
        });
        assert_eq!(worker.repo.enqueue(&job_payload).await.unwrap(), 1);

        let repo = worker.repo.clone();
        let handler = worker.handler.clone();
        let config = worker.config.clone();
        let t = tokio::spawn(async move {
            let res = Worker::run_worker_once(0, &repo, &*handler, &config).await;
            assert_eq!(
                res.unwrap(),
                WorkerStep::Retry {
                    job_id: 1,
                    reason: "alwaysretries".to_string()
                },
            );
            let res = Worker::run_worker_once(0, &repo, &*handler, &config).await;
            assert_eq!(
                res.unwrap(),
                WorkerStep::Retry {
                    job_id: 1,
                    reason: "alwaysretries".to_string()
                },
            );
            let res = Worker::run_worker_once(0, &repo, &*handler, &config).await;
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
        let handler = Arc::new(AlwaysFailsHandler {});
        let worker = Worker {
            num_workers: 1,
            repo: setup().await?,
            handler,
            config: Default::default(),
        };
        let job_payload = JobPayload::Generate(GeneratePayload {
            id: 1,
            min: 10,
            max: 20,
        });
        assert_eq!(worker.repo.enqueue(&job_payload).await.unwrap(), 1);

        let repo = worker.repo.clone();
        let handler = worker.handler.clone();
        let config = worker.config.clone();
        let t = tokio::spawn(async move {
            let res = Worker::run_worker_once(0, &repo, &*handler, &config).await;
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
        let handler = Arc::new(Handler {});
        let repo = setup().await?;
        let worker_1 = Worker {
            num_workers: 1,
            repo: repo.clone(),
            handler: handler.clone(),
            config: Default::default(),
        };
        let worker_2 = Worker {
            num_workers: 1,
            repo,
            handler: handler.clone(),
            config: Default::default(),
        };
        let job_payload = JobPayload::Generate(GeneratePayload {
            id: 1,
            min: 10,
            max: 20,
        });
        assert_eq!(worker_1.repo.enqueue(&job_payload).await.unwrap(), 1);

        let repo_1 = worker_1.repo.clone();
        let handler_1 = worker_1.handler.clone();
        let config_1 = worker_1.config.clone();
        let repo_2 = worker_2.repo.clone();
        let handler_2 = worker_2.handler.clone();
        let config_2 = worker_2.config.clone();

        let t1 = tokio::spawn(async move {
            Worker::run_worker_once(0, &repo_1, &*handler_1, &config_1).await
        });
        let t2 = tokio::spawn(async move {
            Worker::run_worker_once(0, &repo_2, &*handler_2, &config_2).await
        });

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
