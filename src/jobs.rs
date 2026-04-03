use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[repr(i32)]
pub enum JobStatus {
    Pending = 0,
    Leased = 1,
    Succeeded = 2,
    Failed = 3,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ClaimedJob<T> {
    pub queue_id: i64,
    pub attempts: i32,
    pub payload: T,
}

#[cfg(test)]
pub mod test {
    use serde::{Deserialize, Serialize};

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
}
