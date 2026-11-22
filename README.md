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

This stage still returns the unavailable error after storage. It does not yet
verify Stark signatures or the computed commitment root, submit the root
transaction to C1, or forward datasets to C2. It must not be used as a
successful Paradex ingress until those steps are implemented.

`delivery.js` reconstructs v2 records from Mongo, checks chunk and manifest
digests, and calls only C2's dataset import/update RPCs in manifest order. It
does not submit the signed root transaction to C2. When `C2_IMPORT_URL` is
configured, the proxy claims pending admission records with a Mongo lease,
retries failed imports, and marks them delivered only after every dataset RPC
returns successfully. The C1 block replay barrier remains to be connected.

The `Build image` GitHub Actions workflow publishes a versioned GHCR image
manually. The image is linked to this public repository so Kubernetes can pull
it anonymously.
