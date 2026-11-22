import http from 'node:http';

const method = 'madara_V0_1_0_importCommittedDataAndSubmitTransaction';
const maxBytes = 128 * 1024 * 1024;

function reply(res, status, body) {
  res.writeHead(status, { 'content-type': 'application/json' });
  res.end(JSON.stringify(body));
}

export function createServer() {
  return http.createServer(async (req, res) => {
    if (req.method === 'GET' && (req.url === '/health' || req.url === '/ready')) {
      return reply(res, 200, { status: 'ok' });
    }
    if (req.method !== 'POST' || req.url !== '/') {
      return reply(res, 404, { error: 'not found' });
    }

    const chunks = [];
    let size = 0;
    try {
      for await (const chunk of req) {
        size += chunk.length;
        if (size > maxBytes) {
          return reply(res, 413, { error: 'request too large' });
        }
        chunks.push(chunk);
      }
      const call = JSON.parse(Buffer.concat(chunks).toString('utf8'));
      const id = call && Object.hasOwn(call, 'id') ? call.id : null;
      if (call?.jsonrpc !== '2.0' || call?.method !== method ||
          !Array.isArray(call?.params) || call.params.length !== 2 ||
          !Array.isArray(call.params[0]) || call.params[0].length < 1 ||
          call.params[0].length > 4 || typeof call.params[1] !== 'object' ||
          call.params[1] === null) {
        return reply(res, 200, { jsonrpc: '2.0', id,
          error: { code: -32602, message: 'Invalid params or method' } });
      }
      return reply(res, 200, { jsonrpc: '2.0', id,
        error: { code: -32003, message: 'Committed data import is not enabled' } });
    } catch {
      return reply(res, 200, { jsonrpc: '2.0', id: null,
        error: { code: -32700, message: 'Parse error' } });
    }
  });
}

if (process.argv[1] && import.meta.url === new URL(`file://${process.argv[1]}`).href) {
  const port = Number(process.env.PORT || 9945);
  createServer().listen(port, '0.0.0.0', () => {
    console.log(`committed-data proxy listening on ${port}; imports disabled`);
  });
}
