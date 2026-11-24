use mongodb::{
    bson::{doc, DateTime, Document},
    options::{Acknowledgment, CollectionOptions, WriteConcern},
    Client, Collection, Database, IndexModel,
};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub struct Audit {
    database: Database,
    calls: Collection<Document>,
}

impl Audit {
    pub async fn connect(
        uri: &str,
        database: &str,
        collection: &str,
    ) -> mongodb::error::Result<Self> {
        let client = Client::with_uri_str(uri).await?;
        let options = CollectionOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Majority)
                    .journal(true)
                    .build(),
            )
            .build();
        let database = client.database(database);
        let calls = database.collection_with_options(collection, options);
        calls
            .create_index(
                IndexModel::builder()
                    .keys(doc! {"received_at_ns": 1})
                    .build(),
            )
            .await?;
        Ok(Self { database, calls })
    }

    pub async fn record(
        &self,
        received_at: SystemTime,
        method: &str,
        rpc_id: &Value,
        request_bytes: usize,
    ) -> mongodb::error::Result<()> {
        let since_epoch = received_at.duration_since(UNIX_EPOCH).unwrap_or_default();
        let id = rpc_id.to_string();
        self.calls
            .insert_one(doc! {
                "received_at": DateTime::from_millis(i64::try_from(since_epoch.as_millis()).unwrap_or(i64::MAX)),
                "received_at_ns": i64::try_from(since_epoch.as_nanos()).unwrap_or(i64::MAX),
                "method": method,
                "rpc_id": id.chars().take(256).collect::<String>(),
                "request_bytes": i64::try_from(request_bytes).unwrap_or(i64::MAX),
            })
            .await?;
        Ok(())
    }

    pub async fn ready(&self) -> bool {
        self.database.run_command(doc! {"ping": 1}).await.is_ok()
    }
}
