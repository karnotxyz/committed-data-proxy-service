import { randomUUID } from 'node:crypto';

export async function importDataset(c2Url, dataset, fetchImpl = fetch) {
  const update = dataset.update ?? null;
  const method = update
    ? 'madara_V0_1_0_updateCommittedDataSet'
    : 'madara_V0_1_0_importCommittedDataSet';
  const params = update
    ? [update, dataset.root, dataset.entries, dataset.signatures]
    : [dataset.root, dataset.entries, dataset.signatures];
  const response = await fetchImpl(c2Url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: randomUUID(), method, params }),
    signal: AbortSignal.timeout(300000),
  });
  if (!response.ok) throw new Error(`C2 import HTTP ${response.status}`);
  const result = await response.json();
  if (result.error || !Object.hasOwn(result, 'result')) {
    throw new Error(`C2 import rejected: ${result.error?.message || 'invalid response'}`);
  }
  return result.result;
}

export async function deliverDatasets(archive, ids, c2Url, fetchImpl = fetch) {
  for (const id of ids) {
    await importDataset(c2Url, await archive.loadDataset(id), fetchImpl);
  }
}
