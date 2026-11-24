# Committed data proxy service

Stateless ingress for Paradex committed data. C2 Madara validates and stores the
datasets in its own RocksDB and exports them to its MongoDB. This proxy has no
local dataset database or archive. The proxy uses MongoDB only for an ingress
audit record of each committed-data RPC call.

- Port 9945 serves `GET /health`, `GET /ready`, and JSON-RPC `POST /`.
  `madara_V0_1_0_importCommittedDataSet` and
  `madara_V0_1_0_updateCommittedDataSet` relay their request and response to C2.
- For `madara_V0_1_0_importCommittedDataAndSubmitTransaction`, the proxy sends
  each dataset to C2 through its import/update RPC. With
  `REQUIRE_C2_ACK_BEFORE_C1=true` (default), C1 submission waits until every
  dataset is acknowledged. With `false`, delivery continues in memory while
  the signed INVOKE goes to C1 immediately. C2 gets the INVOKE later through
  block replay; the proxy does not send it to C2.
- Port 8080 sends every request except local `/health` and `/ready` to C2's
  feeder gateway and returns C2's status, headers, and body.

Configure `C2_IMPORT_URL` (C2 port 9945), `C2_FEEDER_URL` (C2 port 8080),
`C1_RPC_URL`, `FORWARD_TRANSACTIONS`, `REQUIRE_C2_ACK_BEFORE_C1`, and the required
`MONGO_URI`. `MONGO_DATABASE` defaults to `committed_data`, and
`MONGO_AUDIT_COLLECTION` defaults to `proxy_rpc_calls`. Every call to one of the
three 9945 committed-data methods gets a majority/journal acknowledged MongoDB
document with UTC receive time, nanosecond Unix time, method, JSON-RPC ID, and
request size before forwarding. No dataset payload is stored in this audit
collection. If the audit write fails, the call fails before reaching C1 or C2.
Combined
calls return an error while transaction forwarding is disabled. `/ready` checks
C2's import listener; `/health` is local.

Forwarded requests retry transient transport failures, HTTP 408/429, and HTTP
5xx up to five times with 1, 2, 4, 8, and 16 second delays. JSON-RPC
application errors and other HTTP 4xx responses do not retry.

There is no proxy admission journal or global request ordering. If C1 accepts a
transaction but its response is lost, retries may submit the same signed
transaction again. The caller must reconcile the transaction. With C2
acknowledgment disabled, delivery can be lost if the proxy restarts.

`cargo test` runs unit and process-level HTTP and MongoDB audit tests. The manually triggered
GitHub Actions workflow builds and publishes the public GHCR image.
