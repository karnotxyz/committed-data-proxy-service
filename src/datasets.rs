use crate::{
    patricia::{valid_key, CommittedDataSet, CommittedDataWitness, MAX_COMMITTED_DATA_VALUES},
    request::RawDataset,
};
use mongodb::{
    bson::{doc, spec::BinarySubtype, Binary, Document},
    options::{Acknowledgment, CollectionOptions, ReadConcern, WriteConcern},
    Collection, Database,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use starknet_core::{
    crypto::{ecdsa_verify, Signature},
    utils::starknet_keccak,
};
use starknet_types_core::felt::Felt;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env,
    io::Read,
    sync::Arc,
};
use tokio::sync::Mutex;

pub const VALUES_PER_PAGE: usize = 4096;
pub const MAX_WITNESS_KEYS: usize = 1024;
const CATALOG_PAGE_SIZE: i64 = 64;
type Entry = (Felt, Felt);
type EncodedPage = (String, Option<Vec<u8>>);

#[derive(Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
struct Update {
    application_id: Felt,
    dataset_id: Felt,
    #[serde(deserialize_with = "Option::<Felt>::deserialize")]
    base_root: Option<Felt>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ImportSignature {
    public_key: Felt,
    r: Felt,
    s: Felt,
}

struct Dataset {
    root: Felt,
    entries: Vec<Entry>,
    signatures: Vec<ImportSignature>,
    update: Option<Update>,
}

impl Dataset {
    fn decode(input: RawDataset<'_>) -> Result<Self, String> {
        if input
            .entries
            .as_array()
            .is_none_or(|entries| entries.len() > MAX_COMMITTED_DATA_VALUES)
            || input
                .signatures
                .as_array()
                .is_some_and(|values| values.len() > 32)
        {
            return Err("Too many dataset entries or signatures".into());
        }
        Ok(Self {
            root: Felt::deserialize(input.root).map_err(error_text)?,
            entries: Vec::deserialize(input.entries).map_err(error_text)?,
            signatures: Option::<Vec<_>>::deserialize(input.signatures)
                .map_err(error_text)?
                .unwrap_or_default(),
            update: input
                .update
                .map(Update::deserialize)
                .transpose()
                .map_err(error_text)?,
        })
    }

    fn authorize(&self, chain_id: Felt, allowed: &[Felt]) -> Result<(), String> {
        if allowed.is_empty() {
            return Ok(());
        }
        if self.signatures.is_empty() {
            return Err("Unauthorized dataset import".into());
        }
        // Same domain-separated preimage as the previous proxy and Madara's import RPC.
        let mut preimage = if self.update.is_some() {
            b"MADARA_COMMITTED_DATA_UPDATE_V1".to_vec()
        } else {
            b"MADARA_COMMITTED_DATA_IMPORT_V1".to_vec()
        };
        preimage.extend(chain_id.to_bytes_be());
        if let Some(update) = self.update {
            preimage.extend(update.application_id.to_bytes_be());
            preimage.extend(update.dataset_id.to_bytes_be());
            preimage.push(u8::from(update.base_root.is_some()));
            preimage.extend(update.base_root.unwrap_or(Felt::ZERO).to_bytes_be());
        }
        preimage.extend(self.root.to_bytes_be());
        preimage.extend((self.entries.len() as u32).to_be_bytes());
        let message = starknet_keccak(&preimage);
        let mut seen = HashSet::new();
        for signature in &self.signatures {
            if !allowed.contains(&signature.public_key)
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
                return Err("Unauthorized dataset import".into());
            }
        }
        Ok(())
    }
}

fn error_text(error: impl std::fmt::Display) -> String {
    error.to_string()
}

fn padded(felt: Felt) -> String {
    hex::encode(felt.to_bytes_be())
}

fn root_id(root: Felt) -> String {
    format!("root/{}", padded(root))
}

fn version_id(application: Felt, dataset: Felt, root: Felt) -> String {
    format!(
        "version/{}{}{}",
        padded(application),
        padded(dataset),
        padded(root)
    )
}

#[derive(Deserialize)]
struct Snapshot {
    count: usize,
    pages: Vec<String>,
}

fn page_id(bytes: &[u8]) -> String {
    format!("page/{}", hex::encode(Sha256::digest(bytes)))
}

fn decode_page(id: &str, compressed: &[u8], count: usize) -> Result<Vec<Entry>, String> {
    let size = count * 64;
    let mut raw = Vec::with_capacity(size);
    zstd::stream::read::Decoder::new(compressed)
        .map_err(error_text)?
        .take((size + 1) as u64)
        .read_to_end(&mut raw)
        .map_err(error_text)?;
    if raw.len() != size || page_id(&raw) != id {
        return Err("Stored dataset page length or checksum mismatch".into());
    }
    Ok(raw
        .chunks_exact(64)
        .map(|entry| {
            (
                Felt::from_bytes_be(entry[..32].try_into().unwrap()),
                Felt::from_bytes_be(entry[32..].try_into().unwrap()),
            )
        })
        .collect())
}

struct CachedSnapshot {
    snapshot: Arc<Snapshot>,
    tree: Arc<CommittedDataSet>,
}

pub struct DatasetStore {
    documents: Collection<Document>,
    chain_id: Felt,
    signers: Arc<Vec<Felt>>,
    write: Mutex<()>,
    hot: Mutex<Option<CachedSnapshot>>,
}

impl DatasetStore {
    pub fn from_env(database: &Database) -> Result<Option<Self>, Box<dyn std::error::Error>> {
        match env::var("MONGO_FEEDER_ENABLED").as_deref() {
            Ok("true") => (),
            Err(env::VarError::NotPresent) | Ok("false") => return Ok(None),
            _ => return Err("MONGO_FEEDER_ENABLED must be true or false".into()),
        }
        let chain_id = Felt::from_hex(&env::var("CHAIN_ID")?)?;
        let signers = env::var("COMMITTED_DATA_SIGNERS")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(Felt::from_hex)
            .collect::<Result<_, _>>()?;
        let options = CollectionOptions::builder()
            .read_concern(ReadConcern::majority())
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Majority)
                    .journal(true)
                    .build(),
            )
            .build();
        Ok(Some(Self {
            documents: database.collection_with_options("committed_data_datasets", options),
            chain_id,
            signers: Arc::new(signers),
            write: Mutex::new(()),
            hot: Mutex::new(None),
        }))
    }

    async fn get(&self, id: &str) -> Result<Option<Document>, String> {
        self.documents
            .find_one(doc! {"_id": id})
            .await
            .map_err(error_text)
    }

    async fn insert_if_absent(&self, id: &str, document: Document) -> Result<(), String> {
        self.documents
            .update_one(doc! {"_id": id}, doc! {"$setOnInsert": document})
            .upsert(true)
            .await
            .map_err(error_text)?;
        Ok(())
    }

    async fn snapshot(&self, root: Felt) -> Result<Option<Snapshot>, String> {
        let Some(document) = self.get(&root_id(root)).await? else {
            return Ok(None);
        };
        let snapshot: Snapshot = mongodb::bson::from_document(document).map_err(error_text)?;
        if snapshot.count > MAX_COMMITTED_DATA_VALUES
            || snapshot.pages.len() != snapshot.count.div_ceil(VALUES_PER_PAGE)
            || (snapshot.count == 0) != (root == Felt::ZERO)
        {
            return Err("Invalid stored dataset metadata".into());
        }
        Ok(Some(snapshot))
    }

    async fn tree(
        &self,
        root: Felt,
    ) -> Result<Option<(Arc<Snapshot>, Arc<CommittedDataSet>)>, String> {
        if let Some(cached) = self
            .hot
            .lock()
            .await
            .as_ref()
            .filter(|cached| cached.tree.root() == root)
        {
            return Ok(Some((cached.snapshot.clone(), cached.tree.clone())));
        }
        let Some(snapshot) = self.snapshot(root).await? else {
            return Ok(None);
        };
        let mut cursor = self
            .documents
            .find(doc! {"_id": {"$in": &snapshot.pages}})
            .await
            .map_err(error_text)?;
        let mut compressed = HashMap::new();
        while cursor.advance().await.map_err(error_text)? {
            let document = cursor.deserialize_current().map_err(error_text)?;
            compressed.insert(
                document.get_str("_id").map_err(error_text)?.to_owned(),
                document
                    .get_binary_generic("values")
                    .map_err(error_text)?
                    .clone(),
            );
        }
        let snapshot = Arc::new(snapshot);
        let load = snapshot.clone();
        let tree = tokio::task::spawn_blocking(move || -> Result<_, String> {
            let mut entries = Vec::with_capacity(load.count);
            for (index, id) in load.pages.iter().enumerate() {
                let count = VALUES_PER_PAGE.min(load.count - index * VALUES_PER_PAGE);
                entries.extend(decode_page(
                    id,
                    compressed
                        .remove(id)
                        .ok_or("Missing stored dataset page")?
                        .as_slice(),
                    count,
                )?);
            }
            let tree = CommittedDataSet::new(entries).map_err(error_text)?;
            if tree.root() != root || tree.entries().len() != load.count {
                return Err("Stored dataset root mismatch".into());
            }
            Ok(Arc::new(tree))
        })
        .await
        .map_err(error_text)??;
        *self.hot.lock().await = Some(CachedSnapshot {
            snapshot: snapshot.clone(),
            tree: tree.clone(),
        });
        Ok(Some((snapshot, tree)))
    }

    /// A backfill can skip a dataset only once its materialized root and any namespace label exist.
    pub async fn contains(&self, input: &RawDataset<'_>) -> Result<bool, String> {
        let root = Felt::deserialize(input.root).map_err(error_text)?;
        if let Some(update) = input
            .update
            .map(Update::deserialize)
            .transpose()
            .map_err(error_text)?
        {
            if self
                .get(&version_id(update.application_id, update.dataset_id, root))
                .await?
                .is_none()
            {
                return Ok(false);
            }
        }
        Ok(self.get(&root_id(root)).await?.is_some())
    }

    pub async fn put(&self, raw: RawDataset<'_>) -> Result<(), String> {
        let _writer = self.write.lock().await;
        let dataset = Dataset::decode(raw)?;
        let root = dataset.root;
        let update = dataset.update;
        let dependency = update.and_then(|version| version.base_root.map(|root| (version, root)));
        let base = if let Some((version, root)) = dependency {
            if self
                .get(&version_id(
                    version.application_id,
                    version.dataset_id,
                    root,
                ))
                .await?
                .is_none()
            {
                return Err("Base root is not registered in this namespace".into());
            }
            self.tree(root)
                .await?
                .ok_or("Missing stored base dataset")
                .map(Some)?
        } else {
            None
        };

        let previous = base.as_ref().map(|(manifest, _)| manifest.clone());
        let chain_id = self.chain_id;
        let signers = self.signers.clone();
        let (tree, pages) = tokio::task::spawn_blocking(
            move || -> Result<(Arc<CommittedDataSet>, Vec<EncodedPage>), String> {
                dataset.authorize(chain_id, &signers)?;
                let mut values: BTreeMap<_, _> = base
                    .as_ref()
                    .map(|(_, tree)| tree.entries().iter().copied().collect())
                    .unwrap_or_default();
                let mut seen = HashSet::new();
                for (key, value) in dataset.entries {
                    if !valid_key(key) || !seen.insert(key) {
                        return Err("Invalid or duplicate dataset key".into());
                    }
                    if value == Felt::ZERO {
                        values.remove(&key);
                    } else {
                        values.insert(key, value);
                    }
                }
                let tree =
                    CommittedDataSet::new(values.into_iter().collect()).map_err(error_text)?;
                if tree.root() != root {
                    return Err("Committed-data root mismatch".into());
                }
                let existing: HashSet<_> = previous
                    .as_ref()
                    .into_iter()
                    .flat_map(|manifest| &manifest.pages)
                    .collect();
                let mut pages = Vec::new();
                for entries in tree.entries().chunks(VALUES_PER_PAGE) {
                    let mut raw = Vec::with_capacity(entries.len() * 64);
                    for (key, value) in entries {
                        raw.extend(key.to_bytes_be());
                        raw.extend(value.to_bytes_be());
                    }
                    let id = page_id(&raw);
                    let compressed = if existing.contains(&id) {
                        None
                    } else {
                        Some(zstd::stream::encode_all(raw.as_slice(), 3).map_err(error_text)?)
                    };
                    pages.push((id, compressed));
                }
                Ok((Arc::new(tree), pages))
            },
        )
        .await
        .map_err(error_text)??;

        if let Some(initial) = update.filter(|value| value.base_root.is_none()) {
            let id = format!(
                "namespace/{}{}",
                padded(initial.application_id),
                padded(initial.dataset_id)
            );
            self.insert_if_absent(&id, doc! {"root": format!("{root:#x}")})
                .await?;
            if self
                .get(&id)
                .await?
                .and_then(|doc| doc.get_str("root").ok().map(str::to_owned))
                != Some(format!("{root:#x}"))
            {
                return Err("Dataset already initialized".into());
            }
        }
        let mut ids = Vec::with_capacity(pages.len());
        for (id, compressed) in pages {
            if let Some(bytes) = compressed {
                self.insert_if_absent(
                    &id,
                    doc! {"values": Binary { subtype: BinarySubtype::Generic, bytes }},
                )
                .await?;
            }
            ids.push(id);
        }
        let count = tree.entries().len();
        // Publish root and version only after all referenced pages are majority-acknowledged.
        self.insert_if_absent(
            &root_id(root),
            doc! {"root": format!("{root:#x}"), "count": count as i32, "pages": &ids},
        )
        .await?;
        if let Some(update) = update {
            self.insert_if_absent(&version_id(update.application_id, update.dataset_id, root), doc! {
                "application_id":format!("{:#x}", update.application_id), "dataset_id":format!("{:#x}", update.dataset_id),
                "root":format!("{root:#x}"),
            }).await?;
        }
        *self.hot.lock().await = Some(CachedSnapshot {
            snapshot: Arc::new(Snapshot { count, pages: ids }),
            tree,
        });
        Ok(())
    }

    async fn catalog(
        &self,
        prefix: &str,
        after: Option<String>,
        fields: Document,
    ) -> Result<Vec<Document>, String> {
        let mut cursor = self
            .documents
            .find(doc! {
                "_id": {"$gt": after.as_deref().unwrap_or(prefix), "$lt": format!("{prefix}~")}
            })
            .sort(doc! {"_id":1})
            .limit(CATALOG_PAGE_SIZE)
            .projection(fields)
            .await
            .map_err(error_text)?;
        let mut rows = Vec::new();
        while cursor.advance().await.map_err(error_text)? {
            rows.push(cursor.deserialize_current().map_err(error_text)?);
        }
        Ok(rows)
    }

    pub async fn roots(&self, after: Option<Felt>) -> Result<Vec<String>, String> {
        self.catalog("root/", after.map(root_id), doc! {"root":1})
            .await?
            .iter()
            .map(|row| row.get_str("root").map(str::to_owned).map_err(error_text))
            .collect()
    }

    pub async fn versions(&self, after: Option<(Felt, Felt, Felt)>) -> Result<Vec<Value>, String> {
        self.catalog(
            "version/",
            after.map(|(app, dataset, root)| version_id(app, dataset, root)),
            doc! {"application_id":1,"dataset_id":1,"root":1},
        )
        .await?
        .iter()
        .map(|row| {
            Ok(json!({
                "application_id":row.get_str("application_id").map_err(error_text)?,
                "dataset_id":row.get_str("dataset_id").map_err(error_text)?,
                "root":row.get_str("root").map_err(error_text)?,
            }))
        })
        .collect()
    }

    pub async fn page(&self, root: Felt, start: usize) -> Result<Value, String> {
        let Some(snapshot) = self.snapshot(root).await? else {
            return Ok(Value::Null);
        };
        if snapshot.count == 0 && start == 0 {
            return Ok(json!({"root":root, "start":0, "count":0, "entries":[]}));
        }
        let id = snapshot
            .pages
            .get(start / VALUES_PER_PAGE)
            .ok_or("Committed-data page outside dataset")?;
        let document = self.get(id).await?.ok_or("Missing stored dataset page")?;
        let values = document.get_binary_generic("values").map_err(error_text)?;
        let entries = decode_page(id, values, VALUES_PER_PAGE.min(snapshot.count - start))?;
        Ok(json!({"root":root, "start":start, "count":snapshot.count,"entries":entries}))
    }

    pub async fn witnesses(
        &self,
        root: Felt,
        keys: Vec<Felt>,
    ) -> Result<Option<Vec<CommittedDataWitness>>, String> {
        let Some((_, tree)) = self.tree(root).await? else {
            return Ok(None);
        };
        tokio::task::spawn_blocking(move || {
            Some(
                keys.into_iter()
                    .map(|key| tree.lookup_witness(key))
                    .collect(),
            )
        })
        .await
        .map_err(error_text)
    }
}
