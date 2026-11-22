import { after, before, test } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from './server.js';

const server = createServer();
let url;
before(async () => {
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  url = `http://127.0.0.1:${server.address().port}`;
});
after(() => server.close());

test('health and readiness', async () => {
  for (const path of ['/health', '/ready']) {
    const response = await fetch(url + path);
    assert.equal(response.status, 200);
  }
});

test('accepted method shape fails closed without claiming import', async () => {
  const response = await fetch(url, { method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: 7,
      method: 'madara_V0_1_0_importCommittedDataAndSubmitTransaction',
      params: [[{ root: '0x1', entries: [] }], { type: 'INVOKE' }] }) });
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), { jsonrpc: '2.0', id: 7,
    error: { code: -32003, message: 'Committed data import is not enabled' } });
});

test('Mongo archive finishes before the unavailable response', async () => {
  let staged = false;
  const listener = createServer({ ready: async () => {},
    store: async () => ['manifest-id'],
    stageAdmission: async (tx, ids) => {
      assert.equal(tx.type, 'INVOKE');
      assert.deepEqual(ids, ['manifest-id']);
      staged = true;
    } });
  await new Promise(resolve => listener.listen(0, '127.0.0.1', resolve));
  try {
    const response = await fetch(`http://127.0.0.1:${listener.address().port}`, {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id: 8,
        method: 'madara_V0_1_0_importCommittedDataAndSubmitTransaction',
        params: [[{ root: '0x1', entries: [['0x0', '0x2']] }], { type: 'INVOKE' }] }),
    });
    assert.equal(staged, true);
    assert.equal((await response.json()).error.code, -32003);
  } finally {
    listener.close();
  }
});

test('local smoke endpoint writes and reads MongoDB without accepting external requests', async () => {
  let writes = 0;
  const listener = createServer({ ready: async () => {}, smokeWrite: async () => {
    writes++;
    return 'smoke-id';
  } });
  await new Promise(resolve => listener.listen(0, '127.0.0.1', resolve));
  try {
    const response = await fetch(`http://127.0.0.1:${listener.address().port}/__test/mongo-write`,
      { method: 'POST' });
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), { id: 'smoke-id', stored: true });
    assert.equal(writes, 1);
  } finally {
    listener.close();
  }
});

test('forwarding waits for C2 dataset acknowledgment before submitting only to C1', async () => {
  const calls = [];
  const archive = {
    ready: async () => {},
    store: async () => { calls.push('archive'); return ['manifest']; },
    stageAdmission: async () => { calls.push('admission'); return 'admission'; },
    markC2Delivered: async () => calls.push('c2-recorded'),
    claimSubmission: async () => { calls.push('claim'); return { claimed: true }; },
    finishSubmission: async () => calls.push('c1-recorded'),
  };
  const listener = createServer(archive, {
    forwardTransactions: true, requireC2AckBeforeC1: true,
    c1RpcUrl: 'http://c1/rpc/v0_10', c2ImportUrl: 'http://c2:9945',
    deliver: async () => calls.push('c2-import'),
    submitRoot: async () => { calls.push('c1-submit'); return { transaction_hash: '0x123' }; },
  });
  await new Promise(resolve => listener.listen(0, '127.0.0.1', resolve));
  try {
    const response = await fetch(`http://127.0.0.1:${listener.address().port}/`, {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id: 9,
        method: 'madara_V0_1_0_importCommittedDataAndSubmitTransaction',
        params: [[{ root: '0x0', entries: [] }], { type: 'INVOKE', nonce: '0x0' }] }),
    });
    assert.deepEqual(await response.json(), { jsonrpc: '2.0', id: 9,
      result: { transaction_hash: '0x123' } });
    assert.deepEqual(calls, ['archive', 'admission', 'c2-import', 'c2-recorded',
      'claim', 'c1-submit', 'c1-recorded']);
  } finally {
    listener.close();
  }
});

test('C2 failure prevents C1 submission when acknowledgment is required', async () => {
  let submitted = false;
  const archive = { ready: async () => {}, store: async () => ['manifest'],
    stageAdmission: async () => 'admission' };
  const listener = createServer(archive, { forwardTransactions: true,
    c1RpcUrl: 'http://c1', c2ImportUrl: 'http://c2',
    deliver: async () => { throw new Error('C2 unavailable'); },
    submitRoot: async () => { submitted = true; } });
  await new Promise(resolve => listener.listen(0, '127.0.0.1', resolve));
  try {
    const response = await fetch(`http://127.0.0.1:${listener.address().port}/`, {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id: 10,
        method: 'madara_V0_1_0_importCommittedDataAndSubmitTransaction',
        params: [[{ root: '0x0', entries: [] }], { type: 'INVOKE' }] }),
    });
    assert.equal((await response.json()).error.code, -32005);
    assert.equal(submitted, false);
  } finally {
    listener.close();
  }
});

test('uncertain C1 submission is not sent twice on a retry', async () => {
  let attempts = 0;
  let status = 'archived';
  const archive = { ready: async () => {}, store: async () => ['manifest'],
    stageAdmission: async () => 'admission', markC2Delivered: async () => {},
    claimSubmission: async () => {
      if (status === 'archived') { status = 'submitting'; return { claimed: true }; }
      return { claimed: false, status };
    },
    markSubmissionUnknown: async () => { status = 'uncertain'; } };
  const listener = createServer(archive, { forwardTransactions: true,
    c1RpcUrl: 'http://c1', c2ImportUrl: 'http://c2', deliver: async () => {},
    submitRoot: async () => { attempts++; throw new Error('response lost'); } });
  await new Promise(resolve => listener.listen(0, '127.0.0.1', resolve));
  try {
    const body = JSON.stringify({ jsonrpc: '2.0', id: 11,
      method: 'madara_V0_1_0_importCommittedDataAndSubmitTransaction',
      params: [[{ root: '0x0', entries: [] }], { type: 'INVOKE' }] });
    for (let i = 0; i < 2; i++) {
      const response = await fetch(`http://127.0.0.1:${listener.address().port}/`, {
        method: 'POST', headers: { 'content-type': 'application/json' }, body });
      assert.equal((await response.json()).error.code, -32006);
    }
    assert.equal(attempts, 1);
  } finally {
    listener.close();
  }
});
