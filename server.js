import http from 'node:http';
import { MongoArchive } from './archive.js';
import { deliverDatasets, startDeliveryWorker } from './delivery.js';
import { submitRootTransaction } from './c1.js';

const method = 'madara_V0_1_0_importCommittedDataAndSubmitTransaction';
const maxBytes = 128 * 1024 * 1024;

function reply(res, status, body) {
  res.writeHead(status, { 'content-type': 'application/json' });
  res.end(JSON.stringify(body));
}

export function createServer(archive = null, options = {}) {
  const { forwardTransactions = false, requireC2AckBeforeC1 = true,
    c1RpcUrl, c2ImportUrl, submitRoot = submitRootTransaction,
    deliver = deliverDatasets } = options;
  if (forwardTransactions && (!archive || !c1RpcUrl || !c2ImportUrl)) {
    throw new Error('Forwarding requires MongoDB, C1_RPC_URL and C2_IMPORT_URL');
  }
  return http.createServer(async (req, res) => {
    if (req.method === 'GET' && req.url === '/health') {
      return reply(res, 200, { status: 'ok' });
    }
    if (req.method === 'GET' && req.url === '/ready') {
      try {
        if (archive) await archive.ready();
        return reply(res, 200, { status: 'ok' });
      } catch {
        return reply(res, 503, { status: 'unavailable' });
      }
    }
    if (req.method === 'POST' && req.url === '/__test/mongo-write') {
      if (!['127.0.0.1', '::1', '::ffff:127.0.0.1'].includes(req.socket.remoteAddress)) {
        return reply(res, 404, { error: 'not found' });
      }
      if (!archive) return reply(res, 503, { error: 'MongoDB unavailable' });
      try {
        return reply(res, 200, { id: await archive.smokeWrite(), stored: true });
      } catch (error) {
        console.error('MongoDB smoke write failed:', error.message);
        return reply(res, 503, { error: 'MongoDB write failed' });
      }
    }
    if (req.method !== 'POST' || req.url !== '/') {
      return reply(res, 404, { error: 'not found' });
    }

    const chunks = [];
    let size = 0;
    let id = null;
    try {
      for await (const chunk of req) {
        size += chunk.length;
        if (size > maxBytes) {
          return reply(res, 413, { error: 'request too large' });
        }
        chunks.push(chunk);
      }
      const call = JSON.parse(Buffer.concat(chunks).toString('utf8'));
      id = call && Object.hasOwn(call, 'id') ? call.id : null;
      if (call?.jsonrpc !== '2.0' || call?.method !== method ||
          !Array.isArray(call?.params) || call.params.length !== 2 ||
          !Array.isArray(call.params[0]) || call.params[0].length < 1 ||
          call.params[0].length > 4 || typeof call.params[1] !== 'object' ||
          call.params[1] === null) {
        return reply(res, 200, { jsonrpc: '2.0', id,
          error: { code: -32602, message: 'Invalid params or method' } });
      }
      if (!archive) return reply(res, 200, { jsonrpc: '2.0', id,
        error: { code: -32003, message: 'Committed data import is not enabled' } });
      let datasetIds;
      let admissionId;
      try {
        datasetIds = await archive.store(call.params[0]);
        admissionId = await archive.stageAdmission(call.params[1], datasetIds);
      } catch (error) {
        console.error('Committed data archive failed:', error.message);
        return reply(res, 200, { jsonrpc: '2.0', id,
          error: { code: -32004, message: 'Committed data could not be stored' } });
      }
      if (!forwardTransactions) return reply(res, 200, { jsonrpc: '2.0', id,
        error: { code: -32003, message: 'Committed data import is not enabled' } });
      if (call.params[1].type !== 'INVOKE' || call.params[1].is_query) {
        return reply(res, 200, { jsonrpc: '2.0', id,
          error: { code: -32602, message: 'Expected signed non-query INVOKE' } });
      }
      if (requireC2AckBeforeC1) {
        try {
          await deliver(archive, datasetIds, c2ImportUrl);
          await archive.markC2Delivered(admissionId);
        } catch (error) {
          console.error('C2 dataset import failed:', error.message);
          return reply(res, 200, { jsonrpc: '2.0', id,
            error: { code: -32005, message: 'C2 did not acknowledge committed data' } });
        }
      }
      try {
        const claim = await archive.claimSubmission(admissionId);
        if (!claim.claimed) {
          if (claim.status === 'submitted') {
            return reply(res, 200, { jsonrpc: '2.0', id,
              result: { transaction_hash: claim.transaction_hash } });
          }
          return reply(res, 200, { jsonrpc: '2.0', id,
            error: { code: -32006, message: 'C1 submission is pending reconciliation' } });
        }
        const result = await submitRoot(c1RpcUrl, call.params[1]);
        await archive.finishSubmission(admissionId, result.transaction_hash);
        return reply(res, 200, { jsonrpc: '2.0', id, result });
      } catch (error) {
        console.error('C1 transaction submission uncertain:', error.message);
        await archive.markSubmissionUnknown(admissionId, error.message);
        return reply(res, 200, { jsonrpc: '2.0', id,
          error: { code: -32006, message: 'C1 submission needs reconciliation' } });
      }
    } catch (error) {
      if (!(error instanceof SyntaxError)) console.error('RPC request failed:', error);
      return reply(res, 200, { jsonrpc: '2.0', id,
        error: error instanceof SyntaxError
          ? { code: -32700, message: 'Parse error' }
          : { code: -32603, message: 'Internal error' } });
    }
  });
}

if (process.argv[1] && import.meta.url === new URL(`file://${process.argv[1]}`).href) {
  const port = Number(process.env.PORT || 9945);
  const archive = process.env.MONGO_URI
    ? await MongoArchive.connect(process.env.MONGO_URI,
      process.env.MONGO_DATABASE || 'madara', process.env.CHAIN_ID,
      process.env.MONGO_COLLECTION_PREFIX || 'mempool_transactions')
    : null;
  const forwardTransactions = process.env.FORWARD_TRANSACTIONS === 'true';
  const requireC2AckBeforeC1 = process.env.REQUIRE_C2_ACK_BEFORE_C1 !== 'false';
  const server = createServer(archive, { forwardTransactions, requireC2AckBeforeC1,
    c1RpcUrl: process.env.C1_RPC_URL, c2ImportUrl: process.env.C2_IMPORT_URL });
  if (forwardTransactions && !requireC2AckBeforeC1) {
    startDeliveryWorker(archive, process.env.C2_IMPORT_URL);
  }
  server.listen(port, '0.0.0.0', () => {
    console.log(`committed-data proxy listening on ${port}; forwarding=${forwardTransactions}`);
  });
}
