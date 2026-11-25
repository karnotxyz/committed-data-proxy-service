use mongodb::{
    bson::{doc, Bson, Document},
    error::{Error, ErrorKind, WriteFailure, RETRYABLE_ERROR, RETRYABLE_WRITE_ERROR},
    Collection,
};
use std::{future::Future, time::Duration};

pub const RETRY_DELAYS: [Duration; 5] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(8),
    Duration::from_secs(16),
];
pub const MAJORITY_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, thiserror::Error)]
#[error("MongoDB document is waiting for majority acknowledgement")]
struct PendingMajority;

fn retryable(error: &(dyn std::error::Error + 'static)) -> bool {
    if error.is::<PendingMajority>() {
        return true;
    }
    let Some(error) = error.downcast_ref::<Error>() else {
        return false;
    };
    if error.contains_label(RETRYABLE_WRITE_ERROR) || error.contains_label(RETRYABLE_ERROR) {
        return true;
    }
    match error.kind.as_ref() {
        ErrorKind::Io(_)
        | ErrorKind::DnsResolve { .. }
        | ErrorKind::ServerSelection { .. }
        | ErrorKind::ConnectionPoolCleared { .. } => true,
        // MongoDB transient network, shutdown and replica-set state change codes.
        ErrorKind::Command(error) => matches!(
            error.code,
            6 | 7
                | 89
                | 91
                | 134
                | 189
                | 262
                | 9001
                | 10058
                | 10107
                | 11600
                | 11602
                | 13435
                | 13436
        ),
        ErrorKind::Write(WriteFailure::WriteConcernError(error)) => error.code == 64,
        _ => false,
    }
}

pub async fn mongo_write<T, F>(
    request_id: &str,
    stage: &str,
    mut write: impl FnMut() -> F,
) -> Result<T, BoxError>
where
    F: Future<Output = Result<T, BoxError>>,
{
    for (attempt, delay) in RETRY_DELAYS.into_iter().map(Some).chain([None]).enumerate() {
        match write().await {
            Ok(result) => return Ok(result),
            Err(error) => {
                let Some(delay) = delay.filter(|_| retryable(error.as_ref())) else {
                    return Err(error);
                };
                eprintln!(
                    "request={request_id} MongoDB {stage} retry {}/{} in {}s",
                    attempt + 1,
                    RETRY_DELAYS.len(),
                    delay.as_secs()
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
    unreachable!()
}

/// Idempotent write for a document whose key and contents are fixed across retries.
pub async fn insert_if_absent(
    collection: &Collection<Document>,
    id: impl Into<Bson>,
    document: Document,
) -> Result<(), BoxError> {
    let filter = doc! {"_id":id.into()};
    let result = collection
        .update_one(filter.clone(), doc! {"$setOnInsert":document})
        .upsert(true)
        .await?;
    // A no-op after an uncertain write must also see the document under majority read concern.
    if result.matched_count != 0
        && collection
            .find_one(filter)
            .projection(doc! {"_id":1})
            .await?
            .is_none()
    {
        return Err(PendingMajority.into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{future::ready, io::ErrorKind::ConnectionReset};
    use tokio::time::Instant;

    #[tokio::test(start_paused = true)]
    async fn retries_transient_failures_up_to_five_times() {
        let mut attempts = 0;
        let start = Instant::now();
        mongo_write("test", "archive", || {
            attempts += 1;
            ready(if attempts <= 5 {
                Err(Error::from(ConnectionReset).into())
            } else {
                Ok(())
            })
        })
        .await
        .unwrap();
        assert_eq!(attempts, 6);
        assert_eq!(start.elapsed(), Duration::from_secs(31));

        let mut attempts = 0;
        let result = mongo_write("test", "dataset", || {
            attempts += 1;
            ready(Err::<(), BoxError>(Error::from(ConnectionReset).into()))
        })
        .await;
        assert!(result.is_err());
        assert_eq!(attempts, 6);
    }

    #[tokio::test(start_paused = true)]
    async fn returns_invalid_datasets_immediately() {
        let mut attempts = 0;
        let result = mongo_write("test", "dataset", || {
            attempts += 1;
            ready(Err::<(), BoxError>("Invalid dataset root".into()))
        })
        .await;
        assert_eq!(result.unwrap_err().to_string(), "Invalid dataset root");
        assert_eq!(attempts, 1);
    }
}
