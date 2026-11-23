# Committed data proxy service

Small Rust ingress for Paradex committed data. It uses Madara's pinned Starknet
Poseidon Patricia implementation from `madara-alliance/sequencer@1b51ef1e` to
verify every claimed root and produce authenticated membership and absence
witnesses. It persists the committed-data root and value records in RocksDB
column families with Madara's binary keys. Separate proxy-only families retain
the original signed imports and C1 submission admissions. The database is a
proxy-owned subset of Madara's layout; it is **not** a full Madara database and
must not be mounted directly by a Madara node.

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

`POST /__test/rocksdb-write` on 9945 performs a write/read/delete smoke check
only from loopback. The old `/__test/mongo-write` path remains as a loopback
alias for operational scripts; it now checks RocksDB.

The existing MongoDB deployment can stay running during cutover for its old
archive. This service does not read or migrate that archive. Check the old
archive for pending admissions before switching traffic or deleting MongoDB.

Run `cargo test` locally. The manually triggered GitHub Actions workflow builds
and publishes the public GHCR image.
