# Committed data proxy service

Mainnet ingress for Paradex committed data. The first release exposes the
Madara JSON-RPC method on port 9945 and process health endpoints. It returns
an explicit `-32003` unavailable error for import requests until durable
storage and transaction routing are implemented. It never acknowledges an
import it has not stored.

With `MONGO_URI` and `CHAIN_ID` set, the proxy writes
Madara PR #1361 recovery-v2 content-addressed chunks and manifests to the same
`mempool_transactions_committed_*_v2` collection layout (database `madara`
by default). A separate proxy reference
collection indexes namespace/root to manifest ID and retains import signatures.
Writes use majority reads and journaled majority writes, with the manifest
published only after every chunk is stored. Retried imports are idempotent.
An additional proxy admission collection records the signed transaction and
ordered manifest IDs before any future C1 submission. It is keyed by the
transaction payload digest so a retry cannot silently change its datasets.

By default `FORWARD_TRANSACTIONS` is unset, so the combined RPC archives the
datasets and signed transaction and returns `-32003` without sending anything
to C1 or C2. `POST /__test/mongo-write` works only over loopback; it writes and
reads a random record in a separate Mongo collection with a one-day TTL.

`delivery.js` reconstructs v2 records from Mongo, checks chunk and manifest
digests, and calls only C2's dataset import/update RPCs in manifest order. It
does not submit the signed root transaction to C2. When `C2_IMPORT_URL` is
configured, the proxy claims pending admission records with a Mongo lease,
retries failed imports, and marks them delivered only after every dataset RPC
returns successfully. It never submits the signed root transaction to C2.

When `FORWARD_TRANSACTIONS=true`, set both `C1_RPC_URL` (the Mainnet sequencer's
standard Starknet RPC) and `C2_IMPORT_URL` (C2's committed-data RPC on 9945).
The default `REQUIRE_C2_ACK_BEFORE_C1=true` imports each dataset into C2 before
submitting the signed root INVOKE to C1 via `starknet_addInvokeTransaction`.
The proxy records the returned transaction hash in Mongo and then acknowledges
the combined RPC. A retry returns the previously recorded hash without
resubmission. An uncertain C1 response is left for operator reconciliation,
never blindly retried.

Setting `REQUIRE_C2_ACK_BEFORE_C1=false` submits to C1 without waiting for C2;
a leased Mongo worker delivers the datasets asynchronously. Do not use this
mode until the block replay service has a dataset-delivery barrier. The proxy
does not independently authenticate Stark signatures or calculate the Patricia
root. With the default acknowledgment setting, C2 Madara validates the dataset
before C1 submission. Forwarding stays disabled in production until C2 runs
the committed-data image and the request path is tested end to end.

The `Build image` GitHub Actions workflow publishes a versioned GHCR image
manually. The image is linked to this public repository so Kubernetes can pull
it anonymously.
