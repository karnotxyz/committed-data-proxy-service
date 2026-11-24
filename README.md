# Committed data proxy service

Small Rust ingress for Paradex committed data. It uses Madara's pinned Starknet
Poseidon Patricia implementation from `madara-alliance/sequencer@1b51ef1e` to
verify every claimed root and produce authenticated membership and absence
witnesses. It persists the committed-data root and value records in RocksDB
column families with Madara's binary keys. Separate proxy-only families retain
the original signed imports and C1 submission admissions. The database is a
proxy-owned subset of Madara's layout; it is **not** a full Madara database and
must not be mounted directly by a Madara node.

Each authenticated import also writes a Madara v2 recovery record to the
`committed_data_recovery` column in the same synced RocksDB batch. A background
worker exports its 8,192-entry Zstd chunks and content-addressed manifest to
MongoDB using journaled majority writes and majority reads. The manifest is
published after all chunks; its identity and namespace mapping remain in
RocksDB after the upload payload is acknowledged. Patches keep the exact base
manifest dependency. The local pending payload is bounded to 512 MiB, so an
extended MongoDB outage rejects new imports before the backlog grows without
limit. Imports acknowledge durable local storage; MongoDB export is asynchronous,
as in Madara.

The service listens on two ports:

- `9945`: `GET /health`, `GET /ready`, and JSON-RPC `POST /` for
  `madara_V0_1_0_importCommittedDataAndSubmitTransaction`,
  `madara_V0_1_0_importCommittedDataSet`, and
  `madara_V0_1_0_updateCommittedDataSet`.
- `8080`: `GET /health`, `GET /ready`, and the committed-data feeder routes
  `get_committed_data_roots`, `get_committed_data_versions`,
  `get_committed_data`, `get_committed_data_witness`, and
  `get_committed_data_witnesses` under `/feeder_gateway/`. The last route
  accepts GET and POST.

Reads return Madara's JSON field shapes, root and version pagination limits
(64), value page size (4,096), and authenticated Patricia witnesses. The feeder
has a four-request admission budget and computes proofs on blocking workers.
The dedicated 9945 listener does not expose Starknet user or admin RPC methods.

Set `CHAIN_ID` and `ROCKSDB_PATH` (defaults to `/data/rocksdb`).
Set `MONGO_URI` to enable archive export, `MONGO_DATABASE` (default
`committed_data`), and `MONGO_COLLECTION_PREFIX` (default
`mempool_transactions`). Set `MONGO_REQUIRED=true` in deployments so a missing
URI fails startup. The archive uses Madara's
`<prefix>_committed_chunks_v2` and `<prefix>_committed_manifests_v2` collections;
the proxy does not write Madara's transaction or retention collections.
`COMMITTED_DATA_SIGNERS` is a comma-separated signer allowlist; an empty list
matches Madara's policy of disabling import signature checks. The service checks
Madara's domain-separated Stark ECDSA signature messages when the allowlist is
set. Root publication remains an on-chain transaction, not an import side effect.

`FORWARD_TRANSACTIONS=false` keeps combined calls in archive-only mode and
returns `-32003` after durable storage. When enabled, configure `C1_RPC_URL`
and `C2_IMPORT_URL`. With the default `REQUIRE_C2_ACK_BEFORE_C1=true`, the
proxy imports each dataset to C2 using only its 9945 dataset RPC and waits for
acknowledgment before sending the signed INVOKE to C1. C2 receives the root
transaction later through block replay. With acknowledgment disabled, the
proxy submits to C1 first and a persistent worker retries C2 dataset delivery.
A transaction marked as submitting is never sent to C1 again automatically,
even if its response was lost. Operators must reconcile an uncertain admission.

When forwarding is enabled, combined calls enter one synced RocksDB FIFO before
any dataset import or C1 submission. One worker drains it in admission order;
each HTTP call waits for its usual transaction-hash result. The default and
maximum capacity is 10,000 pending calls (`QUEUE_CAPACITY` can lower it).
At capacity, a new call returns JSON-RPC error `-32007`; retries of an already
queued call join its existing position. The worker resumes pending calls after
a proxy restart. A C2 failure keeps the head in place and retries it, while an
uncertain C1 submission blocks the queue for manual reconciliation. The queue
depth is available at `GET /__queue/status` on port 9945. FIFO order starts
when a complete RPC request is admitted, so senders should avoid concurrent
requests if they need a particular nonce order across the network.

`POST /__test/rocksdb-write` and `POST /__test/mongo-write` on 9945 perform
write/read/delete smoke checks only from loopback. The MongoDB check uses a
separate `<prefix>_proxy_smoke` collection.

Run unit tests with `cargo test --bin committed-data-proxy-service`. The
process-level test needs a disposable MongoDB at `E2E_MONGO_URI` and runs with
`cargo test --test e2e`. The manually triggered GitHub Actions workflow runs
both suites before building and publishing the public GHCR image.
