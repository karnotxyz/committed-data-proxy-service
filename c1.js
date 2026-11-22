import { randomUUID } from 'node:crypto';

/** Submit only the signed root INVOKE to the existing Mainnet sequencer RPC. */
export async function submitRootTransaction(c1RpcUrl, transaction, fetchImpl = fetch) {
  if (!transaction || transaction.type !== 'INVOKE' || transaction.is_query) {
    throw new Error('Expected a non-query signed INVOKE transaction');
  }
  const response = await fetchImpl(c1RpcUrl, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: randomUUID(),
      method: 'starknet_addInvokeTransaction',
      params: { invoke_transaction: transaction } }),
    signal: AbortSignal.timeout(30000),
  });
  if (!response.ok) throw new Error(`C1 submission HTTP ${response.status}`);
  const result = await response.json();
  if (result.error) throw new Error(`C1 submission rejected: ${result.error.message}`);
  if (typeof result.result?.transaction_hash !== 'string') {
    throw new Error('C1 submission returned no transaction hash');
  }
  return { transaction_hash: result.result.transaction_hash };
}
