# Committed data proxy service

Port 9945 accepts Paradex's committed-data JSON-RPC requests at `POST /`.
For `madara_V0_1_0_importCommittedDataAndSubmitTransaction`, the proxy first
archives the signed dataset payloads and the signed INVOKE in MongoDB. With
the MongoDB feeder enabled, the proxy also validates and materializes the
datasets. Once those writes are durable, the original signed INVOKE goes
to C1 through `starknet_addInvokeTransaction`. The result contains C1's
transaction hash.
Standalone `madara_V0_1_0_importCommittedDataSet` and
`madara_V0_1_0_updateCommittedDataSet` acknowledge storage with a null
result. C2 validates and imports datasets later through the replay service.

Port 8080 serves committed-data feeder routes according to
`MONGO_FEEDER_ENABLED`.

- `false` (default) relays feeder requests and responses to `C2_FEEDER_URL`.
- `true` serves roots, versions, data pages, and witnesses directly from
  MongoDB. Set `CHAIN_ID` to the chain ID used to sign datasets and
  `COMMITTED_DATA_SIGNERS` to the comma-separated signer public keys.
  An empty allowlist disables signature checks.

Both ports expose a local `/health` probe. Port 9945 checks MongoDB on
`/ready`. Port 8080 checks its configured feeder backend on `/ready`.

## Configuration and archive

Set `MONGO_URI` and, when `FORWARD_TRANSACTIONS=true`, `C1_RPC_URL`. With
forwarding disabled, recognized calls are still archived and combined
calls return a forwarding-disabled error.

`MONGO_DATABASE` defaults to `committed_data`. `MONGO_AUDIT_COLLECTION`
defaults to `proxy_rpc_calls`. Requests are stored as exact JSON bytes in
ordered 4 MiB binary chunks in `<collection>_chunks`. After all chunks are
durable, the proxy inserts the manifest with method, receive time, byte
count, chunk count, SHA-256, and an indexed `dataset_roots` array of
canonical hexadecimal roots. Writes use majority and journal
acknowledgment. An audit failure prevents C1 submission.

The proxy retries transient MongoDB failures up to five times during
request archival and dataset materialization, with delays of 1, 2, 4, 8,
and 16 seconds. Retries reuse the original request and dataset keys.
Validation failures return immediately. The proxy requires acknowledgement
of both stages before forwarding the INVOKE to C1.

The local feeder stores root and version records and shared data pages in
`committed_data_datasets`. Indexed lookups serve the root and version
lists and retrieve individual data pages. Witnesses use the standard
Patricia tree for the stored root. A materialization failure prevents C1
submission when the local feeder is enabled.

### Signed updates for replay

Set `CHAIN_ID` to the C1 chain ID and supply a shared secret of at least
32 non-whitespace ASCII characters via `COMMITTED_DATA_PROXY_TOKEN`.
This enables `GET /feeder_gateway/get_signed_committed_data_update` on
port 8080 in either feeder mode. Send the token as `Authorization: Bearer`.

- For a C1 publication, query with `root` and `transaction_hash`.
- For a dependency, query with `root`, `application_id`, and `dataset_id`.

The proxy selects one archived request using indexed keys and verifies its
chunks and SHA-256. The response contains `root`, `entries`, `signatures`,
and `update` when the operation is namespace-scoped. A missing lookup
returns HTTP 404.

Run the same binary with `CHAIN_ID` set and `--index-archive` before
enabling replay against an existing archive. It adds the C1 transaction
hash and dependency index fields to legacy manifests. The command can
be rerun and does not start listeners or submit transactions.

### Historical feeder data

To serve historical roots from MongoDB, run the binary with
`MONGO_FEEDER_ENABLED=true` and `--materialize-archive` before switching
port 8080 to local reads. This command checks the archived payloads and
materializes them in receive order. It can be rerun and does not start
listeners or submit transactions.

Forwarded requests retry transient transport errors and HTTP 408, 429 or
5xx responses. A caller that loses a C1 response should reconcile the
original signed transaction on C1.

Run `E2E_MONGO_URI=mongodb://127.0.0.1:27017 cargo test` with a disposable
MongoDB instance. The manually triggered GitHub Actions workflow publishes
the GHCR image.
