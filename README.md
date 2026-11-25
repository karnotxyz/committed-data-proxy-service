# Committed data proxy service

Port 9945 accepts Paradex's committed-data JSON-RPC requests at `POST /`.
For `madara_V0_1_0_importCommittedDataAndSubmitTransaction`, the proxy first
archives the signed dataset payloads and the signed INVOKE in MongoDB. Once
the archive is durable, the original signed INVOKE goes to C1 through
`starknet_addInvokeTransaction`. The result contains C1's transaction hash.
Standalone `madara_V0_1_0_importCommittedDataSet` and
`madara_V0_1_0_updateCommittedDataSet` acknowledge archival with a null
result. C2 validates and imports datasets later through the replay service.

Port 8080 relays feeder requests and responses to C2. Both ports expose a
local `/health` probe. The 9945 `/ready` checks MongoDB and the 8080 `/ready`
checks the C2 feeder.

## Configuration and archive

Set `MONGO_URI`, `C2_FEEDER_URL` (C2 port 8080), and `C1_RPC_URL` when
`FORWARD_TRANSACTIONS=true`. With forwarding disabled, recognized calls
are still archived and combined calls return a forwarding-disabled error.

`MONGO_DATABASE` defaults to `committed_data`. `MONGO_AUDIT_COLLECTION`
defaults to `proxy_rpc_calls`. Requests are stored as exact JSON bytes in
ordered 4 MiB binary chunks in `<collection>_chunks`. After all chunks are
durable, the proxy inserts the manifest with method, receive time, byte
count, chunk count, SHA-256, and an indexed `dataset_roots` array of
canonical hexadecimal roots. Writes use majority and journal
acknowledgment. An audit failure prevents C1 submission.

Run the same binary with `--index-archive` before enabling Mongo-backed
replay on an existing archive. It indexes legacy manifests in place after
checking the ordered chunks and SHA-256. The command can be rerun. It
reads and updates MongoDB without starting listeners.

Forwarded requests retry transient transport errors and HTTP 408, 429 or
5xx responses. A caller that loses a C1 response should reconcile the
original signed transaction on C1.

Run `E2E_MONGO_URI=mongodb://127.0.0.1:27017 cargo test` with a disposable
MongoDB instance. The manually triggered GitHub Actions workflow publishes
the GHCR image.
