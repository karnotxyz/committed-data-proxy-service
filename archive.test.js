import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { zstdDecompressSync } from 'node:zlib';
import { encodeRecoveryRecord } from './archive.js';

test('recovery record uses Madara v1 manifest and 64-byte sorted pairs', () => {
  const record = encodeRecoveryRecord('0x1', {
    root: '0x2', entries: [['0x2', '0x4'], ['0x1', '0x3']],
    update: { application_id: '0x5', dataset_id: '0x6', base_root: null },
  });
  assert.equal(record.manifest.version, 1);
  assert.equal(record.manifest.entries, 2);
  assert.equal(record.manifest.chunks.length, 1);
  assert.equal(record.manifest.chunks[0],
    createHash('sha256').update(record.chunks[0]).digest('hex'));
  assert.equal(record.id, createHash('sha256').update(record.payload).digest('hex'));
  const raw = zstdDecompressSync(record.chunks[0]);
  assert.equal(raw.length, 128);
  assert.equal(raw.subarray(0, 32).toString('hex'), '0'.repeat(63) + '1');
  assert.equal(raw.subarray(32, 64).toString('hex'), '0'.repeat(63) + '3');
});

test('duplicate keys and missing base are rejected', () => {
  assert.throws(() => encodeRecoveryRecord('0x1', {
    root: '0x2', entries: [['0x1', '0x3'], ['0x1', '0x4']],
  }), /Duplicate key/);
  assert.throws(() => encodeRecoveryRecord('0x1', {
    root: '0x2', entries: [['0x1', '0x3']],
    update: { application_id: '0x5', dataset_id: '0x6', base_root: '0x7' },
  }), /Missing recovery base/);
});
