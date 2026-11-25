use crate::request;
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
pub const MAX_REQUEST_BYTES: usize = 128 * 1024 * 1024;

#[derive(Clone)]
pub struct Audit {
    database: Database,
    calls: Collection<Document>,
    chunks: Collection<Document>,
}

impl Audit {
    pub async fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        use std::env;
        Ok(Self::connect(
            &env::var("MONGO_URI")?,
            &env::var("MONGO_DATABASE").unwrap_or_else(|_| "committed_data".into()),
            &env::var("MONGO_AUDIT_COLLECTION").unwrap_or_else(|_| "proxy_rpc_calls".into()),
        )
        .await?)
    }

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
        calls
            .create_index(
                IndexModel::builder()
                    .keys(doc! {"dataset_roots": 1, "_id": -1})
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
        dataset_roots: &[String],
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
                "dataset_roots": dataset_roots.to_vec(),
                "request_bytes": i64::try_from(request_body.len()).unwrap_or(i64::MAX),
                "chunk_count": i32::try_from(request_body.len().div_ceil(CHUNK_BYTES)).unwrap_or(i32::MAX),
                "payload_sha256": Bson::Binary(Binary { subtype: BinarySubtype::Generic, bytes: payload_sha256 }),
            })
            .await?;
        Ok(())
    }

    /// Index legacy manifests in place. Request bytes and chunk documents are immutable.
    pub async fn index_archive(&self) -> Result<u64, Box<dyn std::error::Error>> {
        let mut cursor = self
            .calls
            .find(doc! {"dataset_roots": {"$exists": false}})
            .sort(doc! {"_id":1})
            .await?;
        let mut indexed = 0;
        while cursor.advance().await? {
            let manifest = cursor.deserialize_current()?;
            let id = manifest.get_object_id("_id")?;
            let payload = self
                .read_payload(&manifest, id)
                .await
                .map_err(|error| format!("Audit request {id}: {error}"))?;
            let call: Value = serde_json::from_slice(&payload)?;
            let roots = if call["jsonrpc"] == "2.0" {
                request::parse(&call)
                    .map(|request| request.roots)
                    .unwrap_or_default()
            } else {
                Vec::new()
            };
            let result = self
                .calls
                .update_one(
                    doc! {"_id":id, "dataset_roots":{"$exists":false}},
                    doc! {"$set":{"dataset_roots":roots}},
                )
                .await?;
            indexed += result.modified_count;
        }
        Ok(indexed)
    }

    async fn read_payload(
        &self,
        manifest: &Document,
        id: ObjectId,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let size = usize::try_from(manifest.get_i64("request_bytes")?)?;
        let chunks = usize::try_from(manifest.get_i32("chunk_count")?)?;
        if size == 0 || size > MAX_REQUEST_BYTES || chunks != size.div_ceil(CHUNK_BYTES) {
            return Err("invalid archive size or chunk count".into());
        }
        let mut payload = Vec::with_capacity(size);
        for index in 0..chunks {
            let chunk = self
                .chunks
                .find_one(doc! {"_id":format!("{id}:{index}"), "call_id":id, "index":index as i32})
                .await?
                .ok_or("missing archive chunk")?;
            let bytes = chunk.get_binary_generic("payload")?;
            if bytes.len() != (size - payload.len()).min(CHUNK_BYTES) {
                return Err("invalid archive chunk length".into());
            }
            payload.extend_from_slice(bytes);
        }
        if Sha256::digest(&payload)[..] != manifest.get_binary_generic("payload_sha256")?[..] {
            return Err("archive payload hash mismatch".into());
        }
        Ok(payload)
    }

    pub async fn ready(&self) -> bool {
        self.database.run_command(doc! {"ping": 1}).await.is_ok()
    }
}
