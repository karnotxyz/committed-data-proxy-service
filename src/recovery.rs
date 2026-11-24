//! Madara v2 committed-data recovery format. Field order and compression are wire format.
use crate::store::{Dataset, Update};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use starknet_types_core::felt::Felt;

pub const CHUNK_ENTRIES: usize = 8192;
pub const MAX_COMPRESSED_CHUNK_BYTES: usize = CHUNK_ENTRIES * 64 + 1024;
pub const MAX_MANIFEST_BYTES: usize = 64 * 1024;
pub const DEFAULT_PENDING_BACKUP_BYTES: u64 = 512 * 1024 * 1024;

pub fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecoveryManifest {
    pub version: u32,
    pub chain_id: Felt,
    pub update: Option<Update>,
    pub root: Felt,
    pub base: Option<String>,
    pub entries: u32,
    pub chunks: Vec<String>,
}

impl RecoveryManifest {
    pub fn id(&self) -> Result<String, String> {
        Ok(digest(
            &serde_json::to_vec(self).map_err(|e| e.to_string())?,
        ))
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.version != 2
            || self.entries as usize > crate::patricia::MAX_COMMITTED_DATA_VALUES
            || (self.entries == 0 && self.root != Felt::ZERO)
            || self.chunks.len() != (self.entries as usize).div_ceil(CHUNK_ENTRIES)
            || self.update.as_ref().and_then(|u| u.base_root).is_some() != self.base.is_some()
            || self
                .chunks
                .iter()
                .chain(self.base.iter())
                .any(|id| id.len() != 64 || !id.bytes().all(|b| b.is_ascii_hexdigit()))
        {
            return Err("invalid recovery manifest".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecoveryRecord {
    pub manifest: RecoveryManifest,
    pub chunks: Vec<Vec<u8>>,
}

impl RecoveryRecord {
    pub fn encode(chain_id: Felt, dataset: &Dataset, base: Option<String>) -> Result<Self, String> {
        let mut entries = dataset.entries.clone();
        entries.sort_unstable_by_key(|(key, _)| *key);
        if entries.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err("duplicate recovery key".into());
        }
        let mut chunks = Vec::new();
        for part in entries.chunks(CHUNK_ENTRIES) {
            let mut raw = Vec::with_capacity(part.len() * 64);
            for (key, value) in part {
                raw.extend_from_slice(&key.to_bytes_be());
                raw.extend_from_slice(&value.to_bytes_be());
            }
            chunks.push(zstd::stream::encode_all(raw.as_slice(), 3).map_err(|e| e.to_string())?);
        }
        let manifest = RecoveryManifest {
            version: 2,
            chain_id,
            update: dataset.update.clone(),
            root: dataset.root,
            base,
            entries: entries.len() as u32,
            chunks: chunks.iter().map(|chunk| digest(chunk)).collect(),
        };
        manifest.validate()?;
        Ok(Self { manifest, chunks })
    }
}

pub fn namespace_key(update: Option<&Update>, root: Felt) -> Result<Vec<u8>, String> {
    serde_json::to_vec(&(update.map(|u| (u.application_id, u.dataset_id)), root))
        .map_err(|e| e.to_string())
}
