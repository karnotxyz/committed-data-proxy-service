import { createHash } from 'node:crypto';
import { constants, zstdCompressSync } from 'node:zlib';
import { MongoClient } from 'mongodb';

const prime = (1n << 251n) + (17n << 192n) + 1n;
const maxEntries = 524288;
const chunkEntries = 8192;
const defaultCollectionPrefix = 'mempool_transactions';
const recoveryVersion = 2;
const hash = bytes => createHash('sha256').update(bytes).digest('hex');

function felt(input) {
  if (typeof input !== 'string' || !/^0x[0-9a-fA-F]+$/.test(input)) {
    throw new Error('Expected hex felt');
  }
  const value = BigInt(input);
  if (value < 0n || value >= prime) throw new Error('Felt out of range');
  return `0x${value.toString(16)}`;
}

function feltBytes(value) {
  return Buffer.from(felt(value).slice(2).padStart(64, '0'), 'hex');
}

function normalizeUpdate(input) {
  if (input == null) return null;
  if (typeof input !== 'object' || Array.isArray(input)) throw new Error('Invalid update');
  return {
    application_id: felt(input.application_id),
    dataset_id: felt(input.dataset_id),
    base_root: input.base_root == null ? null : felt(input.base_root),
  };
}

function referenceKey(update, root) {
  return JSON.stringify([update ? [update.application_id, update.dataset_id] : null, root]);
}

export function encodeRecoveryRecord(chainId, dataset, base = null) {
  const root = felt(dataset.root);
  const update = normalizeUpdate(dataset.update);
  if (!Array.isArray(dataset.entries) || dataset.entries.length > maxEntries ||
      (dataset.entries.length === 0 && root !== '0x0')) throw new Error('Invalid entry count');
  if (Boolean(update?.base_root) !== Boolean(base)) throw new Error('Missing recovery base');
  const entries = dataset.entries.map(pair => {
    if (!Array.isArray(pair) || pair.length !== 2) throw new Error('Invalid entry');
    const key = felt(pair[0]);
    if (BigInt(key) >= (1n << 251n)) throw new Error('Patricia key out of range');
    return [key, felt(pair[1])];
  }).sort((a, b) => BigInt(a[0]) < BigInt(b[0]) ? -1 : BigInt(a[0]) > BigInt(b[0]) ? 1 : 0);
  for (let i = 1; i < entries.length; i++) {
    if (entries[i - 1][0] === entries[i][0]) throw new Error('Duplicate key');
  }
  const chunks = [];
  for (let i = 0; i < entries.length; i += chunkEntries) {
    const bytes = Buffer.concat(entries.slice(i, i + chunkEntries)
      .flatMap(([key, value]) => [feltBytes(key), feltBytes(value)]));
    chunks.push(zstdCompressSync(bytes, {
      params: { [constants.ZSTD_c_compressionLevel]: 3 },
    }));
  }
  const manifest = {
    version: recoveryVersion,
    chain_id: felt(chainId),
    update,
    root,
    base,
    entries: entries.length,
    chunks: chunks.map(hash),
  };
  const payload = Buffer.from(JSON.stringify(manifest));
  if (payload.length > 65536) throw new Error('Manifest too large');
  return { id: hash(payload), manifest, payload, chunks };
}

async function immutable(collection, id, payload) {
  await collection.updateOne({ _id: id },
    { $setOnInsert: { _id: id, payload } }, { upsert: true });
  const stored = await collection.findOne({ _id: id });
  if (!stored || !Buffer.from(stored.payload.buffer).equals(payload)) {
    throw new Error('Conflicting immutable archive object');
  }
}

export class MongoArchive {
  constructor(client, database, chainId, collectionPrefix = defaultCollectionPrefix) {
    this.client = client;
    this.chainId = felt(chainId);
    const options = { readConcern: { level: 'majority' },
      writeConcern: { w: 'majority', j: true } };
    const db = client.db(database);
    this.manifests = db.collection(`${collectionPrefix}_committed_manifests_v${recoveryVersion}`, options);
    this.chunks = db.collection(`${collectionPrefix}_committed_chunks_v${recoveryVersion}`, options);
    this.refs = db.collection(`${collectionPrefix}_committed_proxy_refs_v${recoveryVersion}`, options);
    this.admissions = db.collection(`${collectionPrefix}_committed_proxy_admissions_v${recoveryVersion}`, options);
  }

  static async connect(uri, database, chainId, collectionPrefix = defaultCollectionPrefix) {
    const client = new MongoClient(uri, { serverSelectionTimeoutMS: 5000 });
    await client.connect();
    await client.db(database).command({ ping: 1 });
    return new MongoArchive(client, database, chainId, collectionPrefix);
  }

  async ready() {
    await this.client.db().command({ ping: 1 });
  }

  async store(datasets) {
    const ids = [];
    for (const dataset of datasets) {
      if (!Array.isArray(dataset.signatures ?? []) || dataset.signatures?.length > 32) {
        throw new Error('Invalid signature count');
      }
      const update = normalizeUpdate(dataset.update);
      const root = felt(dataset.root);
      let base = null;
      if (update?.base_root) {
        const parent = await this.refs.findOne({ _id: referenceKey(update, update.base_root) });
        if (!parent) throw new Error('Missing recovery base');
        base = parent.manifest_id;
      }
      const record = encodeRecoveryRecord(this.chainId, dataset, base);
      for (let i = 0; i < record.chunks.length; i++) {
        await immutable(this.chunks, record.manifest.chunks[i], record.chunks[i]);
      }
      await immutable(this.manifests, record.id, record.payload);
      const key = referenceKey(update, root);
      await this.refs.updateOne({ _id: key }, { $setOnInsert: {
        _id: key, manifest_id: record.id, signatures: dataset.signatures || [],
      } }, { upsert: true });
      const saved = await this.refs.findOne({ _id: key });
      if (saved?.manifest_id !== record.id) throw new Error('Conflicting recovery root');
      ids.push(record.id);
    }
    return ids;
  }

  async stageAdmission(transaction, datasets) {
    const serialized = Buffer.from(JSON.stringify(transaction));
    if (serialized.length > 1024 * 1024) throw new Error('Transaction too large');
    const id = hash(serialized);
    const document = {
      _id: id,
      chain_id: this.chainId,
      dataset_ids: datasets,
      transaction: serialized,
      status: 'archived',
    };
    await this.admissions.updateOne({ _id: id },
      { $setOnInsert: document }, { upsert: true });
    const saved = await this.admissions.findOne({ _id: id });
    if (!saved || saved.chain_id !== document.chain_id ||
        JSON.stringify(saved.dataset_ids) !== JSON.stringify(datasets) ||
        !Buffer.from(saved.transaction.buffer).equals(serialized)) {
      throw new Error('Conflicting transaction admission');
    }
    return id;
  }
}
