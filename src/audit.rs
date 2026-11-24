use mongodb::{
    bson::{doc, oid::ObjectId, spec::BinarySubtype, Binary, Bson, DateTime, Document},
    options::{Acknowledgment, CollectionOptions, WriteConcern},
    Client, Collection, Database, IndexModel,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

// MongoDB documents are limited to 16 MiB; keep every payload chunk well below it.
const CHUNK_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone)]
pub struct Audit {
    database: Database,
    calls: Collection<Document>,
    chunks: Collection<Document>,
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
        let calls = database.collection_with_options(collection, options.clone());
        let chunks = database.collection_with_options(&format!("{collection}_chunks"), options);
        calls
            .create_index(
                IndexModel::builder()
                    .keys(doc! {"received_at_ns": 1})
                    .build(),
            )
            .await?;
        Ok(Self {
            database,
            calls,
            chunks,
        })
    }

    pub async fn record(
        &self,
        call_id: ObjectId,
        received_at: SystemTime,
        method: &str,
        rpc_id: &Value,
        request_body: &[u8],
    ) -> mongodb::error::Result<()> {
        let since_epoch = received_at.duration_since(UNIX_EPOCH).unwrap_or_default();
        let id = rpc_id.to_string();
        let payload_sha256 = Sha256::digest(request_body).to_vec();
        for (index, bytes) in request_body.chunks(CHUNK_BYTES).enumerate() {
            self.chunks
                .insert_one(doc! {
                    "_id": format!("{}:{index}", call_id.to_hex()),
                    "call_id": call_id.clone(),
                    "index": i32::try_from(index).unwrap_or(i32::MAX),
                    "payload": Bson::Binary(Binary { subtype: BinarySubtype::Generic, bytes: bytes.to_vec() }),
                })
                .await?;
        }
        // Publish the call only after all of its payload chunks are durable.
        self.calls
            .insert_one(doc! {
                "_id": call_id,
                "received_at": DateTime::from_millis(i64::try_from(since_epoch.as_millis()).unwrap_or(i64::MAX)),
                "received_at_ns": i64::try_from(since_epoch.as_nanos()).unwrap_or(i64::MAX),
                "method": method,
                "rpc_id": id.chars().take(256).collect::<String>(),
                "http_method": "POST",
                "path": "/",
                "content_type": "application/json",
                "request_bytes": i64::try_from(request_body.len()).unwrap_or(i64::MAX),
                "chunk_count": i32::try_from(request_body.len().div_ceil(CHUNK_BYTES)).unwrap_or(i32::MAX),
                "payload_sha256": Bson::Binary(Binary { subtype: BinarySubtype::Generic, bytes: payload_sha256 }),
            })
            .await?;
        Ok(())
    }

    pub async fn ready(&self) -> bool {
        self.database.run_command(doc! {"ping": 1}).await.is_ok()
    }
}
