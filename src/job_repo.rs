use serde::{Serialize, de::DeserializeOwned};
use sqlx::SqlitePool;

use crate::{ClaimedJob, JobStatus};

pub struct JobRepo<T> {
    pub pool: SqlitePool,
    pub _marker: std::marker::PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned> JobRepo<T> {
    pub async fn create_tables(&self) -> anyhow::Result<()> {
        const JOBS_SCHEMA: &str = "
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY,
                status INTEGER NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                available_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                leased_until INTEGER,
                last_error TEXT,
                payload TEXT NOT NULL,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
            )
        ";
        let _r = sqlx::query(JOBS_SCHEMA).execute(&self.pool).await?;

        Ok(())
    }

    #[allow(dead_code)]
    pub async fn enqueue(&self, payload: &T) -> anyhow::Result<u64> {
        let payload_json = serde_json::to_value(&payload).unwrap();
        let inserted = sqlx::query(
            "
            INSERT INTO jobs (
                status, attempts, payload
            ) VALUES (?, ?, ?)
        ",
        )
        .bind(JobStatus::Pending as i32)
        .bind(0)
        .bind(payload_json)
        .execute(&self.pool)
        .await?;

        Ok(inserted.rows_affected())
    }

    pub async fn claim_next(
        &self,
        now: i64,
        lease_for_secs: i64,
        max_attempts: i32,
    ) -> anyhow::Result<Option<ClaimedJob<T>>> {
        let claimed_job: Option<(i64, i32, String)> = sqlx::query_as(
            "
            UPDATE jobs
            SET
                status = ?,
                attempts = attempts + 1,
                leased_until = ?,
                updated_at = ?
            WHERE id = (
                SELECT id FROM jobs
                WHERE (status = ? AND attempts < ? AND available_at <= ?)
                OR (status = ? AND attempts < ? AND leased_until < ?)
                ORDER BY available_at ASC, id ASC
                LIMIT 1
            )
            AND (status = ? OR status = ?)
            RETURNING id, attempts, payload
        ",
        )
        .bind(JobStatus::Leased as i32)
        .bind(now + lease_for_secs)
        .bind(now)
        .bind(JobStatus::Pending as i32)
        .bind(max_attempts)
        .bind(now)
        .bind(JobStatus::Leased as i32)
        .bind(max_attempts)
        .bind(now)
        .bind(JobStatus::Pending as i32)
        .bind(JobStatus::Leased as i32)
        .fetch_optional(&self.pool)
        .await?;

        match claimed_job {
            Some(row) => Ok(Some(ClaimedJob {
                queue_id: row.0,
                attempts: row.1,
                payload: serde_json::from_str(&row.2).unwrap(),
            })),
            None => Ok(None),
        }
    }

    pub async fn mark_succeeded(&self, job_id: i64) -> anyhow::Result<()> {
        let _updated = sqlx::query(
            "
            UPDATE jobs
            SET
                status = ?
            WHERE
                id = ?
        ",
        )
        .bind(JobStatus::Succeeded as i32)
        .bind(job_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn mark_failed(&self, job_id: i64) -> anyhow::Result<()> {
        let _updated = sqlx::query(
            "
            UPDATE jobs
            SET
                status = ?
            WHERE
                id = ?
        ",
        )
        .bind(JobStatus::Failed as i32)
        .bind(job_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn requeue(&self, job_id: i64, attempts: i32, error: &str) -> anyhow::Result<()> {
        let updated = sqlx::query(
            "
            UPDATE jobs
            SET
                status = ?,
                attempts = ?,
                last_error = ?
            WHERE
                id = ? AND status = ?
        ",
        )
        .bind(JobStatus::Pending as i32)
        .bind(attempts)
        .bind(error)
        .bind(job_id)
        .bind(JobStatus::Leased as i32)
        .execute(&self.pool)
        .await?;

        if updated.rows_affected() == 1 {
            Ok(())
        } else {
            eprintln!("Failed to requeue job {}. 0 rows were updated.", job_id);
            Err(anyhow::anyhow!(
                "Failed to requeue job {job_id} because 0 rows were updated."
            ))
        }
    }
}

#[cfg(test)]
mod test {
    use std::{marker::PhantomData, str::FromStr};

    use sqlx::sqlite::SqliteConnectOptions;

    use crate::jobs::test::{GeneratePayload, JobPayload};
    use crate::utils::unix_now;

    use super::*;

    async fn setup() -> anyhow::Result<JobRepo<JobPayload>> {
        let opts = SqliteConnectOptions::from_str("sqlite::memory:").unwrap();
        let repo = JobRepo::<JobPayload> {
            pool: SqlitePool::connect_with(opts).await.unwrap(),
            _marker: PhantomData,
        };

        repo.create_tables().await?;

        Ok(repo)
    }

    async fn setup_and_enqueue() -> anyhow::Result<JobRepo<JobPayload>> {
        let repo = setup().await?;
        let job_payload = JobPayload::Generate(GeneratePayload {
            id: 1,
            min: 10,
            max: 20,
        });
        assert_eq!(repo.enqueue(&job_payload).await.unwrap(), 1);
        Ok(repo)
    }

    #[tokio::test]
    async fn enqueue_round_trip() {
        let repo = setup().await.unwrap();
        let job_payload = JobPayload::Generate(GeneratePayload {
            id: 1,
            min: 10,
            max: 20,
        });
        assert_eq!(repo.enqueue(&job_payload).await.unwrap(), 1);

        let row: (String,) = sqlx::query_as("SELECT payload FROM jobs LIMIT 1;")
            .fetch_one(&repo.pool)
            .await
            .unwrap();

        let payload_resp: JobPayload = serde_json::from_str(&row.0).unwrap();

        if let JobPayload::Generate(gen_payload) = job_payload {
            match payload_resp {
                JobPayload::Generate(g) => {
                    assert_eq!(gen_payload.id, g.id);
                    assert_eq!(gen_payload.min, g.min);
                    assert_eq!(gen_payload.max, g.max);
                }
                _ => {}
            }
        }
    }

    #[tokio::test]
    async fn claim_job_works() {
        let repo = setup().await.unwrap();
        let job_payload = JobPayload::Generate(GeneratePayload {
            id: 1,
            min: 10,
            max: 20,
        });
        assert_eq!(repo.enqueue(&job_payload).await.unwrap(), 1);

        let claimed_job = repo.claim_next(unix_now(), 10, 3).await.unwrap();
        match claimed_job {
            Some(claim) => {
                if let JobPayload::Generate(gen_payload) = job_payload {
                    match claim.payload {
                        JobPayload::Generate(g) => {
                            assert_eq!(gen_payload.id, g.id);
                            assert_eq!(gen_payload.min, g.min);
                            assert_eq!(gen_payload.max, g.max);
                        }
                        _ => {}
                    }
                }
            }
            None => {}
        }

        let row: (i32,) = sqlx::query_as("SELECT count(*) FROM jobs WHERE status = ? LIMIT 1;")
            .bind(JobStatus::Pending as i32)
            .fetch_one(&repo.pool)
            .await
            .unwrap();
        assert_eq!(0, row.0);
    }

    #[tokio::test]
    async fn mark_succeeded_works() {
        let repo = setup_and_enqueue().await.unwrap();

        let _ = repo.mark_succeeded(1).await;

        let row: (i32,) = sqlx::query_as("SELECT status FROM jobs LIMIT 1;")
            .fetch_one(&repo.pool)
            .await
            .unwrap();

        assert_eq!(row.0, JobStatus::Succeeded as i32);
    }

    #[tokio::test]
    async fn mark_failed_works() {
        let repo = setup_and_enqueue().await.unwrap();

        let _ = repo.mark_failed(1).await;

        let row: (i32,) = sqlx::query_as("SELECT status FROM jobs LIMIT 1;")
            .fetch_one(&repo.pool)
            .await
            .unwrap();

        assert_eq!(row.0, JobStatus::Failed as i32);
    }

    #[tokio::test]
    async fn requeue_works() {
        let repo = setup_and_enqueue().await.unwrap();

        let claimed_job = repo.claim_next(unix_now(), 10, 3).await.unwrap();
        let last_error = "Test";
        match claimed_job {
            Some(claim) => {
                let _ = repo
                    .requeue(claim.queue_id, claim.attempts + 1, last_error)
                    .await;
                let row: (i32, String, i32) = sqlx::query_as(
                    "SELECT status, last_error, attempts FROM jobs WHERE status = ? LIMIT 1;",
                )
                .bind(JobStatus::Pending as i32)
                .fetch_one(&repo.pool)
                .await
                .unwrap();

                assert_eq!(0, row.0);
                assert_eq!(last_error, row.1);
                assert_eq!(claim.attempts + 1, row.2);
            }
            None => {}
        }
    }

    #[tokio::test]
    async fn leased_job_is_not_requeued() {
        let repo = setup_and_enqueue().await.unwrap();

        let _ = repo.claim_next(unix_now(), 10, 3).await.unwrap();
        let empty_claim = repo.claim_next(unix_now(), 10, 3).await.unwrap();

        assert!(empty_claim.is_none())
    }

    #[tokio::test]
    async fn job_with_expired_lease_is_reclaimed() {
        let repo = setup_and_enqueue().await.unwrap();

        let claim1 = repo.claim_next(unix_now(), 10, 3).await.unwrap().unwrap();
        let claim2 = repo
            .claim_next(unix_now() + 1000, 10, 3)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(claim1.queue_id, claim2.queue_id)
    }

    #[tokio::test]
    async fn max_attempts_works() {
        let repo = setup_and_enqueue().await.unwrap();

        let claim = repo.claim_next(unix_now(), 0, 3).await.unwrap();
        assert!(claim.is_some());
        let claim = repo.claim_next(unix_now() + 1, 0, 3).await.unwrap();
        assert!(claim.is_some());
        let claim = repo.claim_next(unix_now() + 2, 0, 3).await.unwrap();
        assert!(claim.is_some());
    }

    #[tokio::test]
    async fn no_claim_if_max_attempts_exceeded() {
        let repo = setup_and_enqueue().await.unwrap();

        let claim = repo.claim_next(unix_now(), 0, 2).await.unwrap();
        assert!(claim.is_some());
        let claim = repo.claim_next(unix_now() + 1, 0, 2).await.unwrap();
        assert!(claim.is_some());
        let claim = repo.claim_next(unix_now() + 2, 0, 2).await.unwrap();
        assert!(claim.is_none());
    }
}
