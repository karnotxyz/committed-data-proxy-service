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

export async function deliverOnePending(archive, c2Url, fetchImpl = fetch) {
  const admission = await archive.claimDelivery();
  if (!admission) return false;
  try {
    await deliverDatasets(archive, admission.dataset_ids, c2Url, fetchImpl);
    await archive.finishDelivery(admission._id, admission.lease_token);
  } catch (error) {
    await archive.finishDelivery(admission._id, admission.lease_token, error.message, admission.attempts);
    throw error;
  }
  return true;
}

export function startDeliveryWorker(archive, c2Url) {
  let running = false;
  const timer = setInterval(async () => {
    if (running) return;
    running = true;
    try {
      await deliverOnePending(archive, c2Url);
    } catch (error) {
      console.error('C2 delivery failed:', error.message);
    } finally {
      running = false;
    }
  }, 2000);
  return () => clearInterval(timer);
}
