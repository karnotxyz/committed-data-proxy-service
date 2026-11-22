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
  let stored = false;
  const listener = createServer({ ready: async () => {}, store: async () => { stored = true; } });
  await new Promise(resolve => listener.listen(0, '127.0.0.1', resolve));
  try {
    const response = await fetch(`http://127.0.0.1:${listener.address().port}`, {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id: 8,
        method: 'madara_V0_1_0_importCommittedDataAndSubmitTransaction',
        params: [[{ root: '0x1', entries: [['0x0', '0x2']] }], { type: 'INVOKE' }] }),
    });
    assert.equal(stored, true);
    assert.equal((await response.json()).error.code, -32003);
  } finally {
    listener.close();
  }
});
