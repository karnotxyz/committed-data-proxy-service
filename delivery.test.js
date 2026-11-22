import { test } from 'node:test';
import assert from 'node:assert/strict';
import { deliverDatasets, deliverOnePending } from './delivery.js';

test('C2 receives only ordered dataset import RPCs', async () => {
  const sent = [];
  const archive = { loadDataset: async id => id === 'first'
    ? { root: '0x1', entries: [['0x0', '0x2']], signatures: [] }
    : { update: { application_id: '0x3', dataset_id: '0x4', base_root: '0x1' },
      root: '0x5', entries: [['0x0', '0x6']], signatures: [] } };
  const fakeFetch = async (_url, options) => {
    sent.push(JSON.parse(options.body));
    return { ok: true, json: async () => ({ jsonrpc: '2.0', result: null }) };
  };
  await deliverDatasets(archive, ['first', 'second'], 'http://c2', fakeFetch);
  assert.deepEqual(sent.map(call => call.method), [
    'madara_V0_1_0_importCommittedDataSet',
    'madara_V0_1_0_updateCommittedDataSet',
  ]);
  assert.equal(sent[0].params.length, 3);
  assert.equal(sent[1].params.length, 4);
  assert.equal(sent.some(call => call.method.includes('SubmitTransaction')), false);
});

test('delivery marks admission after all dataset imports', async () => {
  const calls = [];
  const archive = {
    claimDelivery: async () => ({ _id: 'admission', lease_token: 'lease', dataset_ids: ['one'] }),
    loadDataset: async () => ({ root: '0x1', entries: [], signatures: [] }),
    finishDelivery: async (...args) => calls.push(args),
  };
  await deliverOnePending(archive, 'http://c2', async () => ({
    ok: true, json: async () => ({ jsonrpc: '2.0', result: null }),
  }));
  assert.deepEqual(calls, [['admission', 'lease']]);
});
