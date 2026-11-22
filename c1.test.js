import { test } from 'node:test';
import assert from 'node:assert/strict';
import { submitRootTransaction } from './c1.js';

test('C1 receives signed root INVOKE over standard Starknet RPC', async () => {
  const tx = { type: 'INVOKE', nonce: '0x7', signature: ['0x1', '0x2'] };
  let submitted;
  const result = await submitRootTransaction('http://c1/rpc/v0_10', tx,
    async (_url, options) => {
      submitted = JSON.parse(options.body);
      return { ok: true, json: async () => ({ result: { transaction_hash: '0x123' } }) };
    });
  assert.equal(submitted.method, 'starknet_addInvokeTransaction');
  assert.deepEqual(submitted.params, { invoke_transaction: tx });
  assert.deepEqual(result, { transaction_hash: '0x123' });
});

test('query transactions are never submitted to C1', async () => {
  await assert.rejects(submitRootTransaction('http://c1',
    { type: 'INVOKE', is_query: true }, () => { throw new Error('must not call'); }),
  /non-query/);
});
