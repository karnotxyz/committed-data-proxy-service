//! Madara's immutable MongoDB manifest/chunk archive, with majority and journal acknowledgments.
use crate::recovery::{
    digest, RecoveryManifest, RecoveryRecord, MAX_COMPRESSED_CHUNK_BYTES, MAX_MANIFEST_BYTES,
};
use mongodb::{
    bson::{doc, spec::BinarySubtype, Binary, Bson, Document},
    options::{Acknowledgment, CollectionOptions, ReadConcern, WriteConcern},
    Collection,
};

pub struct Archive {
    manifests: Collection<Document>,
    chunks: Collection<Document>,
    smoke: Collection<Document>,
}

impl Archive {
    pub fn new(database: &mongodb::Database, prefix: &str) -> Self {
        let options = CollectionOptions::builder()
            .read_concern(ReadConcern::majority())
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Majority)
                    .journal(true)
                    .build(),
            )
            .build();
        Self {
            manifests: database.collection_with_options(
                &format!("{prefix}_committed_manifests_v2"),
                options.clone(),
            ),
            chunks: database
                .collection_with_options(&format!("{prefix}_committed_chunks_v2"), options),
            smoke: database.collection(&format!("{prefix}_proxy_smoke")),
        }
    }

    pub async fn smoke_write(&self) -> Result<String, String> {
        let id = format!("smoke/{}", uuid::Uuid::new_v4());
        self.smoke
            .insert_one(doc! {"_id": &id, "value": "ok"})
            .await
            .map_err(|e| e.to_string())?;
        let found = self
            .smoke
            .find_one(doc! {"_id": &id})
            .await
            .map_err(|e| e.to_string())?;
        self.smoke
            .delete_one(doc! {"_id": &id})
            .await
            .map_err(|e| e.to_string())?;
        if found.as_ref().and_then(|doc| doc.get_str("value").ok()) != Some("ok") {
            return Err("MongoDB smoke readback failed".into());
        }
        Ok(id)
    }

    fn document(id: &str, bytes: Vec<u8>) -> Document {
        doc! {"_id":id, "payload": Bson::Binary(Binary { subtype: BinarySubtype::Generic, bytes })}
    }

    async fn immutable(
        collection: &Collection<Document>,
        id: &str,
        bytes: Vec<u8>,
    ) -> Result<(), String> {
        let document = Self::document(id, bytes);
        collection
            .update_one(doc! {"_id":id}, doc! {"$setOnInsert":document.clone()})
            .upsert(true)
            .await
            .map_err(|e| e.to_string())?;
        if collection
            .find_one(doc! {"_id":id})
            .await
            .map_err(|e| e.to_string())?
            .as_ref()
            != Some(&document)
        {
            return Err("conflicting immutable backup object".into());
        }
        Ok(())
    }

    pub async fn complete(&self, id: &str) -> Result<bool, String> {
        let Some(document) = self
            .manifests
            .find_one(doc! {"_id":id})
            .await
            .map_err(|e| e.to_string())?
        else {
            return Ok(false);
        };
        let bytes = document
            .get_binary_generic("payload")
            .map_err(|e| e.to_string())?;
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err("oversized recovery manifest".into());
        }
        let manifest: RecoveryManifest =
            serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
        manifest.validate()?;
        if manifest.id()? != id {
            return Err("recovery manifest digest mismatch".into());
        }
        Ok(true)
    }

    pub async fn upload(&self, record: &RecoveryRecord) -> Result<(), String> {
        record.manifest.validate()?;
        if let Some(base) = &record.manifest.base {
            if !self.complete(base).await? {
                return Err("backup base is incomplete".into());
            }
        }
        if record.chunks.len() != record.manifest.chunks.len() {
            return Err("incomplete backup record".into());
        }
        for (hash, chunk) in record.manifest.chunks.iter().zip(&record.chunks) {
            if chunk.len() > MAX_COMPRESSED_CHUNK_BYTES || digest(chunk) != *hash {
                return Err("invalid backup chunk".into());
            }
            Self::immutable(&self.chunks, hash, chunk.clone()).await?;
        }
        Self::immutable(
            &self.manifests,
            &record.manifest.id()?,
            serde_json::to_vec(&record.manifest).map_err(|e| e.to_string())?,
        )
        .await
    }
}
