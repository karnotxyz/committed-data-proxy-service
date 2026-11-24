use crate::patricia::{
    valid_key, CommittedDataSet, CommittedDataWitness, MAX_COMMITTED_DATA_VALUES,
};
use crate::recovery::{
    namespace_key, RecoveryManifest, RecoveryRecord, DEFAULT_PENDING_BACKUP_BYTES,
};
use bincode::Options as _;
use rocksdb::{
    ColumnFamilyDescriptor, Direction, IteratorMode, Options, WriteBatch, WriteOptions, DB,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use starknet_core::{
    crypto::{ecdsa_verify, Signature},
    utils::starknet_keccak,
};
use starknet_types_core::felt::Felt;
use std::{
    collections::{BTreeMap, HashSet},
    path::Path,
    sync::Mutex,
};
use uuid::Uuid;

const ROOTS: &str = "committed_data_roots";
const VALUES: &str = "committed_data_values";
const IMPORTS: &str = "committed_data_proxy_imports";
const ADMISSIONS: &str = "committed_data_proxy_admissions";
const RECOVERY: &str = "committed_data_recovery";
const QUEUE: &str = "committed_data_proxy_queue";
const QUEUE_NEXT: &[u8] = b"meta/next";
const QUEUE_COUNT: &[u8] = b"meta/count";
pub const MAX_QUEUE_CAPACITY: u64 = 10_000;
const ROOT_PREFIX: &[u8] = b"committed_data_mpt/";
const VERSION_PREFIX: &[u8] = b"committed_data_versions/";
const COMPLETE_PREFIX: &[u8] = b"cdv/complete/";
pub const ROOTS_PER_PAGE: usize = 64;
pub const VALUES_PER_PAGE: usize = 4096;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Update {
    pub application_id: Felt,
    pub dataset_id: Felt,
    #[serde(deserialize_with = "Option::<Felt>::deserialize")]
    pub base_root: Option<Felt>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportSignature {
    pub public_key: Felt,
    pub r: Felt,
    pub s: Felt,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Dataset {
    #[serde(default)]
    pub update: Option<Update>,
    pub root: Felt,
    pub entries: Vec<(Felt, Felt)>,
    #[serde(default)]
    pub signatures: Vec<ImportSignature>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Admission {
    pub dataset_ids: Vec<String>,
    pub transaction: serde_json::Value,
    pub status: String,
    pub c2_delivered: bool,
    pub transaction_hash: Option<String>,
    pub attempts: u32,
    pub next_attempt_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueuedCall {
    pub admission_id: String,
    pub datasets: Vec<Dataset>,
    pub transaction: serde_json::Value,
}

pub enum QueueEnqueue {
    Pending(u64),
    Full,
    Conflict,
}

pub struct Store {
    db: DB,
    chain_id: Felt,
    allowed_signers: Vec<Felt>,
    writer: Mutex<()>,
}

fn recovery_key(prefix: u8, id: &[u8]) -> Vec<u8> {
    [&[prefix], id].concat()
}

fn queue_key(seq: u64) -> Vec<u8> {
    [b"q/".as_slice(), &seq.to_be_bytes()].concat()
}

fn encode_recovery<T: Serialize>(value: &T) -> Result<Vec<u8>, String> {
    bincode::DefaultOptions::new()
        .serialize(value)
        .map_err(|e| e.to_string())
}

fn decode_recovery<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    bincode::DefaultOptions::new()
        .deserialize(bytes)
        .map_err(|e| e.to_string())
}

fn cf<'a>(db: &'a DB, name: &str) -> Result<&'a rocksdb::ColumnFamily, String> {
    db.cf_handle(name)
        .ok_or_else(|| format!("missing column {name}"))
}

fn root_key(root: Felt) -> Vec<u8> {
    [ROOT_PREFIX, &root.to_bytes_be(), &[255, 0, 0, 0, 0]].concat()
}

fn version_key(update: &Update, root: Felt) -> Vec<u8> {
    [
        VERSION_PREFIX,
        &update.application_id.to_bytes_be(),
        &update.dataset_id.to_bytes_be(),
        &root.to_bytes_be(),
    ]
    .concat()
}

fn value_key(root: Felt, key: Felt) -> Vec<u8> {
    [root.to_bytes_be().as_slice(), key.to_bytes_be().as_slice()].concat()
}

fn decode_felt(bytes: &[u8]) -> Result<Felt, String> {
    let bytes: [u8; 32] = bytes.try_into().map_err(|_| "invalid felt length")?;
    let value = Felt::from_bytes_be(&bytes);
    if value.to_bytes_be() != bytes {
        return Err("noncanonical felt".into());
    }
    Ok(value)
}

fn synced() -> WriteOptions {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    options
}

impl Store {
    pub fn open(path: &Path, chain_id: Felt, allowed_signers: Vec<Felt>) -> Result<Self, String> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let columns: Vec<_> = [
            "default", ROOTS, VALUES, IMPORTS, ADMISSIONS, RECOVERY, QUEUE,
        ]
        .into_iter()
        .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
        .collect();
        let db = DB::open_cf_descriptors(&options, path, columns).map_err(|e| e.to_string())?;
        Ok(Self {
            db,
            chain_id,
            allowed_signers,
            writer: Mutex::new(()),
        })
    }

    fn authorize(&self, dataset: &Dataset) -> Result<(), String> {
        if dataset.signatures.len() > 32 {
            return Err("too many signatures".into());
        }
        if self.allowed_signers.is_empty() {
            return Ok(());
        }
        if dataset.signatures.is_empty() {
            return Err("unauthorized import".into());
        }
        let mut preimage = if dataset.update.is_some() {
            b"MADARA_COMMITTED_DATA_UPDATE_V1".to_vec()
        } else {
            b"MADARA_COMMITTED_DATA_IMPORT_V1".to_vec()
        };
        preimage.extend(self.chain_id.to_bytes_be());
        if let Some(update) = &dataset.update {
            preimage.extend(update.application_id.to_bytes_be());
            preimage.extend(update.dataset_id.to_bytes_be());
            preimage.push(u8::from(update.base_root.is_some()));
            preimage.extend(update.base_root.unwrap_or(Felt::ZERO).to_bytes_be());
        }
        preimage.extend(dataset.root.to_bytes_be());
        preimage.extend(
            u32::try_from(dataset.entries.len())
                .map_err(|e| e.to_string())?
                .to_be_bytes(),
        );
        let message = starknet_keccak(&preimage);
        let mut seen = HashSet::new();
        for signature in &dataset.signatures {
            if !self.allowed_signers.contains(&signature.public_key)
                || !seen.insert(signature.public_key)
                || !ecdsa_verify(
                    &signature.public_key,
                    &message,
                    &Signature {
                        r: signature.r,
                        s: signature.s,
                    },
                )
                .unwrap_or(false)
            {
                return Err("unauthorized import".into());
            }
        }
        Ok(())
    }

    fn count(&self, root: Felt) -> Result<Option<u32>, String> {
        let value = self
            .db
            .get_cf(cf(&self.db, ROOTS)?, root_key(root))
            .map_err(|e| e.to_string())?;
        value
            .map(|bytes| {
                if bytes.len() != 21 {
                    return Err("invalid root metadata".into());
                }
                Ok(u32::from_be_bytes(bytes[..4].try_into().unwrap()))
            })
            .transpose()
    }

    fn entries(&self, root: Felt) -> Result<Option<Vec<(Felt, Felt)>>, String> {
        let Some(count) = self.count(root)? else {
            return Ok(None);
        };
        let mut entries = Vec::with_capacity(count as usize);
        for row in self.db.iterator_cf(
            cf(&self.db, VALUES)?,
            IteratorMode::From(&root.to_bytes_be(), Direction::Forward),
        ) {
            let (key, value) = row.map_err(|e| e.to_string())?;
            if !key.starts_with(&root.to_bytes_be()) {
                break;
            }
            if key.len() != 64 || value.len() < 34 {
                return Err("invalid committed-data value".into());
            }
            entries.push((decode_felt(&key[32..])?, decode_felt(&value[..32])?));
            if entries.len() > MAX_COMMITTED_DATA_VALUES {
                return Err("oversized dataset".into());
            }
        }
        if entries.len() != count as usize {
            return Err("incomplete dataset".into());
        }
        Ok(Some(entries))
    }

    pub fn put(&self, dataset: &Dataset) -> Result<String, String> {
        let _guard = self.writer.lock().map_err(|e| e.to_string())?;
        if dataset.entries.len() > MAX_COMMITTED_DATA_VALUES {
            return Err("oversized dataset".into());
        }
        self.authorize(dataset)?;
        let mut merged = if let Some(base) = dataset.update.as_ref().and_then(|u| u.base_root) {
            let update = dataset.update.as_ref().unwrap();
            if self
                .db
                .get_cf(cf(&self.db, ROOTS)?, version_key(update, base))
                .map_err(|e| e.to_string())?
                .is_none()
            {
                return Err("unknown namespace base root".into());
            }
            self.entries(base)?
                .ok_or("missing base dataset")?
                .into_iter()
                .collect::<BTreeMap<_, _>>()
        } else {
            BTreeMap::new()
        };
        if let Some(update) = &dataset.update {
            if update.base_root.is_none()
                && self
                    .db
                    .iterator_cf(
                        cf(&self.db, ROOTS)?,
                        IteratorMode::From(
                            &version_key(update, Felt::ZERO)[..VERSION_PREFIX.len() + 64],
                            Direction::Forward,
                        ),
                    )
                    .next()
                    .transpose()
                    .map_err(|e| e.to_string())?
                    .is_some_and(|(key, _)| {
                        key.starts_with(
                            &version_key(update, Felt::ZERO)[..VERSION_PREFIX.len() + 64],
                        ) && key.as_ref() != version_key(update, dataset.root)
                    })
            {
                return Err("dataset already initialized".into());
            }
        }
        let mut seen = HashSet::new();
        for (key, value) in &dataset.entries {
            if !valid_key(*key) || !seen.insert(*key) {
                return Err("invalid or duplicate key".into());
            }
            if *value == Felt::ZERO {
                merged.remove(key);
            } else {
                merged.insert(*key, *value);
            }
        }
        let tree =
            CommittedDataSet::new(merged.into_iter().collect()).map_err(|e| e.to_string())?;
        if tree.root() != dataset.root {
            return Err("committed-data root mismatch".into());
        }
        let payload = serde_json::to_vec(dataset).map_err(|e| e.to_string())?;
        let id = hex::encode(Sha256::digest(&payload));
        let recovery_cf = cf(&self.db, RECOVERY)?;
        let namespace = namespace_key(dataset.update.as_ref(), dataset.root)?;
        let identity_key = recovery_key(b'n', &namespace);
        let existing_recovery = self
            .db
            .get_cf(recovery_cf, &identity_key)
            .map_err(|e| e.to_string())?;
        let recovery = if existing_recovery.is_none() {
            let base = dataset
                .update
                .as_ref()
                .and_then(|u| u.base_root)
                .map(|root| {
                    let key = recovery_key(b'n', &namespace_key(dataset.update.as_ref(), root)?);
                    self.db
                        .get_cf(recovery_cf, key)
                        .map_err(|e| e.to_string())?
                        .ok_or_else(|| "missing base recovery identity".to_owned())
                        .and_then(|bytes| {
                            String::from_utf8(bytes.to_vec()).map_err(|e| e.to_string())
                        })
                })
                .transpose()?;
            let record = RecoveryRecord::encode(self.chain_id, dataset, base)?;
            let record_id = record.manifest.id()?;
            let record_bytes = encode_recovery(&record)?;
            let pending = self
                .pending_recovery_bytes()?
                .checked_add(record_bytes.len() as u64)
                .ok_or("recovery backlog overflow")?;
            if pending > DEFAULT_PENDING_BACKUP_BYTES {
                return Err("committed-data backup backlog full; retry after export".into());
            }
            Some((record, record_id, record_bytes, pending))
        } else {
            None
        };
        let mut batch = WriteBatch::default();
        if self.count(dataset.root)?.is_none() {
            let mut metadata = Vec::with_capacity(21);
            metadata.extend(
                u32::try_from(tree.entries().len())
                    .map_err(|e| e.to_string())?
                    .to_be_bytes(),
            );
            metadata.extend([0; 17]); // Madara root metadata: no checkpoint, no persistent proof pointer.
            batch.put_cf(cf(&self.db, ROOTS)?, root_key(dataset.root), metadata);
            batch.put_cf(
                cf(&self.db, ROOTS)?,
                [COMPLETE_PREFIX, &dataset.root.to_bytes_be()].concat(),
                [1],
            );
            for (key, value) in tree.entries() {
                let depth = tree.lookup_witness(*key).siblings.len() as u16;
                let row = [
                    value.to_bytes_be().as_slice(),
                    depth.to_be_bytes().as_slice(),
                ]
                .concat();
                batch.put_cf(cf(&self.db, VALUES)?, value_key(dataset.root, *key), row);
            }
        }
        if let Some(update) = &dataset.update {
            batch.put_cf(cf(&self.db, ROOTS)?, version_key(update, dataset.root), []);
        }
        batch.put_cf(cf(&self.db, IMPORTS)?, id.as_bytes(), payload);
        if let Some((record, record_id, bytes, pending)) = recovery {
            batch.put_cf(recovery_cf, identity_key, record_id.as_bytes());
            batch.put_cf(
                recovery_cf,
                recovery_key(b'm', record_id.as_bytes()),
                encode_recovery(&record.manifest)?,
            );
            batch.put_cf(
                recovery_cf,
                recovery_key(b'r', record_id.as_bytes()),
                &bytes,
            );
            batch.put_cf(
                recovery_cf,
                recovery_key(b'p', record_id.as_bytes()),
                encode_recovery(&(bytes.len() as u64))?,
            );
            batch.put_cf(recovery_cf, b"bytes", encode_recovery(&pending)?);
        }
        self.db
            .write_opt(batch, &synced())
            .map_err(|e| e.to_string())?;
        Ok(id)
    }

    fn pending_recovery_bytes(&self) -> Result<u64, String> {
        match self
            .db
            .get_cf(cf(&self.db, RECOVERY)?, b"bytes")
            .map_err(|e| e.to_string())?
        {
            Some(bytes) => decode_recovery(&bytes),
            None => Ok(0),
        }
    }

    pub fn pending_recovery(&self, limit: usize) -> Result<Vec<(String, RecoveryRecord)>, String> {
        let cf = cf(&self.db, RECOVERY)?;
        let mut pending = Vec::new();
        for row in self
            .db
            .iterator_cf(cf, IteratorMode::From(b"p", Direction::Forward))
        {
            let (key, _) = row.map_err(|e| e.to_string())?;
            if !key.starts_with(b"p") || pending.len() == limit {
                break;
            }
            let id = String::from_utf8(key[1..].to_vec()).map_err(|e| e.to_string())?;
            let bytes = self
                .db
                .get_cf(cf, recovery_key(b'r', id.as_bytes()))
                .map_err(|e| e.to_string())?
                .ok_or("missing recovery payload")?;
            let record: RecoveryRecord = decode_recovery(&bytes)?;
            if record.manifest.id()? != id {
                return Err("corrupt recovery identity".into());
            }
            pending.push((id, record));
        }
        Ok(pending)
    }

    pub fn recovery_record(&self, id: &str) -> Result<Option<RecoveryRecord>, String> {
        self.db
            .get_cf(cf(&self.db, RECOVERY)?, recovery_key(b'r', id.as_bytes()))
            .map_err(|e| e.to_string())?
            .map(|bytes| decode_recovery(&bytes))
            .transpose()
    }

    pub fn recovery_manifest(&self, id: &str) -> Result<Option<RecoveryManifest>, String> {
        self.db
            .get_cf(cf(&self.db, RECOVERY)?, recovery_key(b'm', id.as_bytes()))
            .map_err(|e| e.to_string())?
            .map(|bytes| decode_recovery(&bytes))
            .transpose()
    }

    pub fn mark_recovery_exported(&self, id: &str) -> Result<(), String> {
        let _guard = self.writer.lock().map_err(|e| e.to_string())?;
        let cf = cf(&self.db, RECOVERY)?;
        let key = recovery_key(b'p', id.as_bytes());
        let Some(size) = self.db.get_cf(cf, &key).map_err(|e| e.to_string())? else {
            return Ok(());
        };
        let size: u64 = decode_recovery(&size)?;
        let pending = self
            .pending_recovery_bytes()?
            .checked_sub(size)
            .ok_or("recovery backlog underflow")?;
        let mut batch = WriteBatch::default();
        batch.delete_cf(cf, key);
        batch.delete_cf(cf, recovery_key(b'r', id.as_bytes()));
        batch.put_cf(cf, b"bytes", encode_recovery(&pending)?);
        self.db
            .write_opt(batch, &synced())
            .map_err(|e| e.to_string())
    }

    pub fn load_import(&self, id: &str) -> Result<Dataset, String> {
        let bytes = self
            .db
            .get_cf(cf(&self.db, IMPORTS)?, id.as_bytes())
            .map_err(|e| e.to_string())?
            .ok_or("missing import")?;
        serde_json::from_slice(&bytes).map_err(|e| e.to_string())
    }

    pub fn roots(&self, after: Option<Felt>) -> Result<Vec<Felt>, String> {
        let start = after.map(root_key).unwrap_or(ROOT_PREFIX.to_vec());
        let mut roots = Vec::new();
        for row in self.db.iterator_cf(
            cf(&self.db, ROOTS)?,
            IteratorMode::From(&start, Direction::Forward),
        ) {
            let (key, _) = row.map_err(|e| e.to_string())?;
            if !key.starts_with(ROOT_PREFIX) {
                break;
            }
            if key.len() != ROOT_PREFIX.len() + 37 {
                return Err("invalid root key".into());
            }
            if after.is_some() && key.as_ref() == start {
                continue;
            }
            roots.push(decode_felt(
                &key[ROOT_PREFIX.len()..ROOT_PREFIX.len() + 32],
            )?);
            if roots.len() == ROOTS_PER_PAGE {
                break;
            }
        }
        Ok(roots)
    }

    pub fn versions(
        &self,
        after: Option<(Felt, Felt, Felt)>,
    ) -> Result<Vec<(Felt, Felt, Felt)>, String> {
        let start = after
            .map(|(a, d, r)| {
                version_key(
                    &Update {
                        application_id: a,
                        dataset_id: d,
                        base_root: None,
                    },
                    r,
                )
            })
            .unwrap_or(VERSION_PREFIX.to_vec());
        let mut versions = Vec::new();
        for row in self.db.iterator_cf(
            cf(&self.db, ROOTS)?,
            IteratorMode::From(&start, Direction::Forward),
        ) {
            let (key, _) = row.map_err(|e| e.to_string())?;
            if !key.starts_with(VERSION_PREFIX) {
                break;
            }
            if after.is_some() && key.as_ref() == start {
                continue;
            }
            if key.len() != VERSION_PREFIX.len() + 96 {
                return Err("invalid version key".into());
            }
            let k = &key[VERSION_PREFIX.len()..];
            versions.push((
                decode_felt(&k[..32])?,
                decode_felt(&k[32..64])?,
                decode_felt(&k[64..])?,
            ));
            if versions.len() == ROOTS_PER_PAGE {
                break;
            }
        }
        Ok(versions)
    }

    pub fn page(&self, root: Felt, start: usize) -> Result<Option<serde_json::Value>, String> {
        if !start.is_multiple_of(VALUES_PER_PAGE) || start >= MAX_COMMITTED_DATA_VALUES {
            return Err("invalid page offset".into());
        }
        let Some(entries) = self.entries(root)? else {
            return Ok(None);
        };
        if start >= entries.len() && !(start == 0 && entries.is_empty()) {
            return Err("page outside dataset".into());
        }
        Ok(Some(
            serde_json::json!({ "root": root, "start": start, "count": entries.len(),
            "entries": &entries[start..entries.len().min(start + VALUES_PER_PAGE)] }),
        ))
    }

    pub fn witnesses(
        &self,
        root: Felt,
        keys: &[Felt],
    ) -> Result<Option<Vec<CommittedDataWitness>>, String> {
        if keys.is_empty() || keys.len() > 1024 || keys.iter().any(|key| !valid_key(*key)) {
            return Err("invalid witness keys".into());
        }
        let Some(entries) = self.entries(root)? else {
            return Ok(None);
        };
        let tree = CommittedDataSet::new(entries).map_err(|e| e.to_string())?;
        if tree.root() != root {
            return Err("stored root mismatch".into());
        }
        Ok(Some(
            keys.iter().map(|key| tree.lookup_witness(*key)).collect(),
        ))
    }

    pub fn admission(&self, id: &str) -> Result<Option<Admission>, String> {
        self.db
            .get_cf(cf(&self.db, ADMISSIONS)?, id.as_bytes())
            .map_err(|e| e.to_string())?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(|e| e.to_string()))
            .transpose()
    }

    pub fn save_admission(&self, id: &str, admission: &Admission) -> Result<(), String> {
        self.db
            .put_cf_opt(
                cf(&self.db, ADMISSIONS)?,
                id.as_bytes(),
                serde_json::to_vec(admission).map_err(|e| e.to_string())?,
                &synced(),
            )
            .map_err(|e| e.to_string())
    }

    fn queue_counter(&self, key: &[u8]) -> Result<u64, String> {
        let Some(bytes) = self
            .db
            .get_cf(cf(&self.db, QUEUE)?, key)
            .map_err(|e| e.to_string())?
        else {
            return Ok(0);
        };
        let bytes: [u8; 8] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| "invalid queue counter")?;
        Ok(u64::from_be_bytes(bytes))
    }

    pub fn queue_len(&self) -> Result<u64, String> {
        self.queue_counter(QUEUE_COUNT)
    }

    pub fn enqueue(&self, call: &QueuedCall, capacity: u64) -> Result<QueueEnqueue, String> {
        let _guard = self.writer.lock().map_err(|e| e.to_string())?;
        let queue = cf(&self.db, QUEUE)?;
        let index = [b"i/".as_slice(), call.admission_id.as_bytes()].concat();
        let encoded = serde_json::to_vec(call).map_err(|e| e.to_string())?;
        if let Some(seq) = self.db.get_cf(queue, &index).map_err(|e| e.to_string())? {
            let seq: [u8; 8] = seq
                .as_slice()
                .try_into()
                .map_err(|_| "invalid queue index")?;
            let seq = u64::from_be_bytes(seq);
            let existing = self
                .db
                .get_cf(queue, queue_key(seq))
                .map_err(|e| e.to_string())?
                .ok_or("missing indexed queue item")?;
            return Ok(if existing.as_ref() == encoded.as_slice() {
                QueueEnqueue::Pending(seq)
            } else {
                QueueEnqueue::Conflict
            });
        }
        let count = self.queue_len()?;
        if count >= capacity {
            return Ok(QueueEnqueue::Full);
        }
        let seq = self.queue_counter(QUEUE_NEXT)?;
        let next = seq.checked_add(1).ok_or("queue sequence exhausted")?;
        let mut batch = WriteBatch::default();
        batch.put_cf(queue, queue_key(seq), encoded);
        batch.put_cf(queue, index, seq.to_be_bytes());
        batch.put_cf(queue, QUEUE_NEXT, next.to_be_bytes());
        batch.put_cf(queue, QUEUE_COUNT, (count + 1).to_be_bytes());
        self.db
            .write_opt(batch, &synced())
            .map_err(|e| e.to_string())?;
        Ok(QueueEnqueue::Pending(seq))
    }

    pub fn queue_head(&self) -> Result<Option<(u64, QueuedCall)>, String> {
        let queue = cf(&self.db, QUEUE)?;
        let Some(row) = self
            .db
            .iterator_cf(queue, IteratorMode::From(b"q/", Direction::Forward))
            .next()
        else {
            return Ok(None);
        };
        let (key, value) = row.map_err(|e| e.to_string())?;
        if !key.starts_with(b"q/") {
            return Ok(None);
        }
        let seq: [u8; 8] = key[2..].try_into().map_err(|_| "invalid queue key")?;
        let call = serde_json::from_slice(&value).map_err(|e| e.to_string())?;
        Ok(Some((u64::from_be_bytes(seq), call)))
    }

    pub fn finish_queue_head(&self, seq: u64, admission_id: &str) -> Result<(), String> {
        let _guard = self.writer.lock().map_err(|e| e.to_string())?;
        let Some((head, call)) = self.queue_head()? else {
            return Err("queue head is missing".into());
        };
        if head != seq || call.admission_id != admission_id {
            return Err("queue order changed".into());
        }
        let count = self
            .queue_len()?
            .checked_sub(1)
            .ok_or("queue count underflow")?;
        let queue = cf(&self.db, QUEUE)?;
        let mut batch = WriteBatch::default();
        batch.delete_cf(queue, queue_key(seq));
        batch.delete_cf(queue, [b"i/".as_slice(), admission_id.as_bytes()].concat());
        batch.put_cf(queue, QUEUE_COUNT, count.to_be_bytes());
        self.db
            .write_opt(batch, &synced())
            .map_err(|e| e.to_string())
    }

    pub fn smoke_write(&self) -> Result<String, String> {
        let id = format!("smoke/{}", Uuid::new_v4());
        self.db
            .put_opt(id.as_bytes(), b"ok", &synced())
            .map_err(|e| e.to_string())?;
        let read = self.db.get(id.as_bytes()).map_err(|e| e.to_string())?;
        if read.as_deref() != Some(b"ok") {
            return Err("RocksDB smoke readback failed".into());
        }
        self.db.delete(id.as_bytes()).map_err(|e| e.to_string())?;
        Ok(id)
    }

    pub fn pending_admissions(&self) -> Result<Vec<(String, Admission)>, String> {
        let mut pending = Vec::new();
        for row in self
            .db
            .iterator_cf(cf(&self.db, ADMISSIONS)?, IteratorMode::Start)
        {
            let (key, value) = row.map_err(|e| e.to_string())?;
            let admission: Admission = serde_json::from_slice(&value).map_err(|e| e.to_string())?;
            if !admission.c2_delivered {
                pending.push((String::from_utf8_lossy(&key).into_owned(), admission));
            }
        }
        Ok(pending)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn durable_queue_is_fifo_deduplicated_and_bounded() {
        assert_eq!(MAX_QUEUE_CAPACITY, 10_000);
        let dir = tempdir().unwrap();
        let first = QueuedCall {
            admission_id: "first".into(),
            datasets: vec![],
            transaction: serde_json::json!({"nonce":"0x1"}),
        };
        let second = QueuedCall {
            admission_id: "second".into(),
            datasets: vec![],
            transaction: serde_json::json!({"nonce":"0x2"}),
        };
        let third = QueuedCall {
            admission_id: "third".into(),
            datasets: vec![],
            transaction: serde_json::json!({"nonce":"0x3"}),
        };
        {
            let db = Store::open(dir.path(), Felt::ONE, vec![]).unwrap();
            assert!(matches!(
                db.enqueue(&first, 2).unwrap(),
                QueueEnqueue::Pending(0)
            ));
            assert!(matches!(
                db.enqueue(&second, 2).unwrap(),
                QueueEnqueue::Pending(1)
            ));
            assert!(matches!(
                db.enqueue(&first, 2).unwrap(),
                QueueEnqueue::Pending(0)
            ));
            let mut conflict = first.clone();
            conflict.transaction = serde_json::json!({"nonce":"0x99"});
            assert!(matches!(
                db.enqueue(&conflict, 2).unwrap(),
                QueueEnqueue::Conflict
            ));
            assert!(matches!(db.enqueue(&third, 2).unwrap(), QueueEnqueue::Full));
            assert_eq!(db.queue_len().unwrap(), 2);
        }
        let db = Store::open(dir.path(), Felt::ONE, vec![]).unwrap();
        assert_eq!(db.queue_head().unwrap().unwrap().1.admission_id, "first");
        db.finish_queue_head(0, "first").unwrap();
        assert_eq!(db.queue_head().unwrap().unwrap().1.admission_id, "second");
        db.finish_queue_head(1, "second").unwrap();
        assert_eq!(db.queue_len().unwrap(), 0);
        assert!(matches!(
            db.enqueue(&third, 2).unwrap(),
            QueueEnqueue::Pending(2)
        ));
    }

    fn felt_decimal(value: &str) -> Felt {
        Felt::from_dec_str(value).unwrap()
    }

    #[test]
    fn madara_vector_survives_restart_with_pages_and_authenticated_witnesses() {
        let dir = tempdir().unwrap();
        let root = felt_decimal(
            "3000094458541565324654730242423920536387969495385651240652446843684607154951",
        );
        let dataset = Dataset {
            update: None,
            root,
            entries: vec![(Felt::TWO, Felt::from(100))],
            signatures: vec![],
        };
        let id = {
            let db = Store::open(dir.path(), Felt::ONE, vec![]).unwrap();
            let id = db.put(&dataset).unwrap();
            let pending = db.pending_recovery(10).unwrap();
            assert_eq!(pending.len(), 1);
            let manifest = &pending[0].1.manifest;
            assert_eq!(manifest.version, 2);
            assert_eq!(manifest.chain_id, Felt::ONE);
            assert_eq!(manifest.root, root);
            assert_eq!(manifest.entries, 1);
            assert_eq!(manifest.chunks.len(), 1);
            assert_eq!(
                db.recovery_manifest(&pending[0].0)
                    .unwrap()
                    .unwrap()
                    .id()
                    .unwrap(),
                pending[0].0
            );
            assert_eq!(db.roots(None).unwrap(), vec![root]);
            let page = db.page(root, 0).unwrap().unwrap();
            assert_eq!(page["count"], 1);
            assert_eq!(page["entries"], serde_json::json!([["0x2", "0x64"]]));
            assert!(db
                .witnesses(root, &[Felt::TWO, Felt::THREE])
                .unwrap()
                .unwrap()
                .iter()
                .all(|w| w.verify()));
            id
        };
        let reopened = Store::open(dir.path(), Felt::ONE, vec![]).unwrap();
        let pending = reopened.pending_recovery(10).unwrap();
        assert_eq!(pending.len(), 1);
        reopened.mark_recovery_exported(&pending[0].0).unwrap();
        reopened.mark_recovery_exported(&pending[0].0).unwrap();
        assert!(reopened.pending_recovery(10).unwrap().is_empty());
        assert!(reopened.recovery_manifest(&pending[0].0).unwrap().is_some());
        assert_eq!(reopened.load_import(&id).unwrap().root, root);
        assert_eq!(reopened.roots(None).unwrap(), vec![root]);
        assert!(reopened.witnesses(root, &[Felt::THREE]).unwrap().unwrap()[0].verify());
    }

    #[test]
    fn namespace_update_uses_retained_base_and_rejects_wrong_root() {
        let dir = tempdir().unwrap();
        let db = Store::open(dir.path(), Felt::ONE, vec![]).unwrap();
        let first = felt_decimal(
            "3000094458541565324654730242423920536387969495385651240652446843684607154951",
        );
        let second = felt_decimal(
            "276299005619304478918798114286346347891379591787649727104272896463670221328",
        );
        let update = Update {
            application_id: Felt::ONE,
            dataset_id: Felt::TWO,
            base_root: None,
        };
        db.put(&Dataset {
            update: Some(update.clone()),
            root: first,
            entries: vec![(Felt::TWO, Felt::from(100))],
            signatures: vec![],
        })
        .unwrap();
        let patch = Dataset {
            update: Some(Update {
                base_root: Some(first),
                ..update
            }),
            root: second,
            entries: vec![
                (Felt::THREE, Felt::from(125)),
                (Felt::from(10), Felt::from(200)),
            ],
            signatures: vec![],
        };
        db.put(&patch).unwrap();
        let pending = db.pending_recovery(10).unwrap();
        assert_eq!(pending.len(), 2);
        let parent = pending
            .iter()
            .find(|(_, record)| record.manifest.root == first)
            .unwrap()
            .0
            .clone();
        let child = pending
            .iter()
            .find(|(_, record)| record.manifest.root == second)
            .unwrap();
        assert_eq!(child.1.manifest.base.as_deref(), Some(parent.as_str()));
        assert_eq!(child.1.manifest.entries, 2); // Back up submitted upserts, not the merged map.
        assert_eq!(db.versions(None).unwrap().len(), 2);
        assert_eq!(db.page(second, 0).unwrap().unwrap()["count"], 3);
        let bad = Dataset {
            root: Felt::ONE,
            ..patch
        };
        assert!(db.put(&bad).unwrap_err().contains("root mismatch"));
    }

    #[test]
    fn signer_policy_uses_madara_import_message_and_rejects_tampering() {
        let dir = tempdir().unwrap();
        let public_key = starknet_types_core::curve::AffinePoint::generator().x();
        let db = Store::open(dir.path(), Felt::ONE, vec![public_key]).unwrap();
        let root = felt_decimal(
            "3000094458541565324654730242423920536387969495385651240652446843684607154951",
        );
        let mut preimage = b"MADARA_COMMITTED_DATA_IMPORT_V1".to_vec();
        preimage.extend(Felt::ONE.to_bytes_be());
        preimage.extend(root.to_bytes_be());
        preimage.extend(1u32.to_be_bytes());
        let signature =
            starknet_core::crypto::ecdsa_sign(&Felt::ONE, &starknet_keccak(&preimage)).unwrap();
        let dataset = Dataset {
            update: None,
            root,
            entries: vec![(Felt::TWO, Felt::from(100))],
            signatures: vec![ImportSignature {
                public_key,
                r: signature.r,
                s: signature.s,
            }],
        };
        db.put(&dataset).unwrap();
        let mut bad = dataset;
        bad.signatures[0].s = Felt::ZERO;
        assert_eq!(db.put(&bad).unwrap_err(), "unauthorized import");
    }
}
