use crate::{
    datasets::DatasetStore,
    invoke_hash::invoke_hash,
    request::{self, ArchivedRequest},
    retry::{insert_if_absent, BoxError, MAJORITY_WRITE_TIMEOUT},
    signed_updates::{version_key, Lookup},
};
use mongodb::{
    bson::{doc, oid::ObjectId, spec::BinarySubtype, Binary, Bson, DateTime, Document},
    options::{Acknowledgment, CollectionOptions, ReadConcern, WriteConcern},
    Client, Collection, Database, IndexModel,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use starknet_types_core::felt::Felt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
            .read_concern(ReadConcern::majority())
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Majority)
                    .journal(true)
                    .w_timeout(MAJORITY_WRITE_TIMEOUT)
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
        for keys in [
            doc! {"c1_transaction_hash": 1, "dataset_roots": 1, "_id": -1},
            doc! {"dataset_versions": 1, "_id": 1},
        ] {
            calls
                .create_index(IndexModel::builder().keys(keys).build())
                .await?;
        }
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
        index: &Document,
    ) -> Result<(), BoxError> {
        let since_epoch = received_at.duration_since(UNIX_EPOCH).unwrap_or_default();
        let id = rpc_id.to_string();
        let payload_sha256 = Sha256::digest(request_body).to_vec();
        for (index, bytes) in request_body.chunks(CHUNK_BYTES).enumerate() {
            insert_if_absent(
                &self.chunks,
                format!("{}:{index}", call_id.to_hex()),
                doc! {
                    "call_id": call_id,
                    "index": i32::try_from(index).unwrap_or(i32::MAX),
                    "payload": Bson::Binary(Binary { subtype: BinarySubtype::Generic, bytes: bytes.to_vec() }),
                },
            ).await?;
        }
        // Publish the call only after all of its payload chunks are durable.
        let mut manifest = index.clone();
        manifest.extend(doc! {
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
        });
        insert_if_absent(&self.calls, call_id, manifest).await
    }

    /// Index legacy manifests in place. Request bytes and chunk documents are immutable.
    pub async fn index_archive(
        &self,
        chain_id: Option<Felt>,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let needs_index = if chain_id.is_some() {
            "dataset_versions"
        } else {
            "dataset_roots"
        };
        let missing = doc! {needs_index: {"$exists": false}};
        let mut cursor = self
            .calls
            .find(missing.clone())
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
            let parsed = (call["jsonrpc"] == "2.0")
                .then(|| request::parse(&call).ok())
                .flatten();
            let mut filter = missing.clone();
            filter.insert("_id", id);
            let result = self
                .calls
                .update_one(
                    filter,
                    doc! {"$set":archive_index(parsed.as_ref(), chain_id)},
                )
                .await?;
            indexed += result.modified_count;
        }
        Ok(indexed)
    }

    pub async fn signed_dataset(
        &self,
        lookup: Lookup,
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(payload) = tokio::time::timeout(Duration::from_secs(45), async {
            let (key, order) = lookup.mongo_filter();
            let Some(manifest) = self.calls.find_one(key).sort(doc! {"_id": order}).await? else {
                return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(None);
            };
            self.read_payload(&manifest, manifest.get_object_id("_id")?)
                .await
                .map(Some)
        })
        .await??
        else {
            return Ok(None);
        };
        tokio::task::spawn_blocking(move || {
            let call: Value = serde_json::from_slice(&payload)?;
            let request = request::parse(&call)?;
            let dataset = request
                .datasets
                .into_iter()
                .find(|dataset| lookup.matches(dataset))
                .ok_or("Dataset index does not match archived request")?;
            let bytes = serde_json::to_vec(&dataset)?;
            Ok(Some(bytes))
        })
        .await?
    }

    pub fn database(&self) -> &Database {
        &self.database
    }

    pub async fn materialize_archive(
        &self,
        store: &DatasetStore,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let mut cursor = self
            .calls
            .find(doc! {})
            .sort(doc! {"received_at_ns":1, "_id":1})
            .allow_disk_use(true)
            .await?;
        let mut imported = 0;
        while cursor.advance().await? {
            let manifest = cursor.deserialize_current()?;
            let id = manifest.get_object_id("_id")?;
            let payload = self
                .read_payload(&manifest, id)
                .await
                .map_err(|error| format!("Audit request {id}: {error}"))?;
            let call: Value = serde_json::from_slice(&payload)?;
            if call["jsonrpc"] != "2.0" {
                continue;
            }
            let Ok(request) = request::parse(&call) else {
                continue;
            };
            for dataset in request.datasets {
                if !store
                    .contains(&dataset)
                    .await
                    .map_err(|error| format!("Audit request {id}: {error}"))?
                {
                    store
                        .put(dataset)
                        .await
                        .map_err(|error| format!("Audit request {id}: {error}"))?;
                    imported += 1;
                }
            }
        }
        Ok(imported)
    }

    async fn read_payload(
        &self,
        manifest: &Document,
        id: ObjectId,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
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

pub fn archive_index(parsed: Option<&ArchivedRequest<'_>>, chain_id: Option<Felt>) -> Document {
    let roots = parsed
        .map(|parsed| parsed.roots.as_slice())
        .unwrap_or_default();
    let mut index = doc! {"dataset_roots": roots.to_vec()};
    if let Some(chain_id) = chain_id {
        let versions: Vec<String> = parsed
            .into_iter()
            .flat_map(|parsed| &parsed.datasets)
            .filter_map(version_key)
            .collect();
        let transaction = parsed
            .and_then(|parsed| parsed.transaction)
            .and_then(|transaction| invoke_hash(transaction, chain_id));
        index.insert("dataset_versions", versions);
        index.insert("c1_transaction_hash", transaction);
    }
    index
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn retry_resumes_an_archive_after_a_partial_write() {
        let uri = std::env::var("E2E_MONGO_URI").expect("set E2E_MONGO_URI");
        let name = format!("proxy_partial_archive_{}", std::process::id());
        let audit = Audit::connect(&uri, &name, "calls").await.unwrap();
        let id = ObjectId::new();
        let received = SystemTime::now();
        let body = vec![42; CHUNK_BYTES + 7];
        let index = doc! {"dataset_roots": ["0x1"]};
        audit.chunks.insert_one(doc! {
            "_id": format!("{id}:0"),
            "call_id": id,
            "index": 0,
            "payload": Binary {subtype: BinarySubtype::Generic, bytes: body[..CHUNK_BYTES].to_vec()}
        }).await.unwrap();

        for _ in 0..2 {
            audit
                .record(id, received, request::IMPORT, &Value::Null, &body, &index)
                .await
                .unwrap();
        }
        assert_eq!(
            audit
                .chunks
                .count_documents(doc! {"call_id":id})
                .await
                .unwrap(),
            2
        );
        assert_eq!(audit.calls.count_documents(doc! {}).await.unwrap(), 1);
        let manifest = audit
            .calls
            .find_one(doc! {"_id":id})
            .await
            .unwrap()
            .unwrap();
        assert_eq!(audit.read_payload(&manifest, id).await.unwrap(), body);
        audit.database.drop().await.unwrap();
    }
}
