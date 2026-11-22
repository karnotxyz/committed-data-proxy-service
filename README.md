# Committed data proxy service

Mainnet ingress for Paradex committed data. The first release exposes the
Madara JSON-RPC method on port 9945 and process health endpoints. It returns
an explicit `-32003` unavailable error for import requests until durable
storage and transaction routing are implemented. It never acknowledges an
import it has not stored.

With `MONGO_URI`, `MONGO_DATABASE`, and `CHAIN_ID` set, the proxy writes
Madara recovery-v1 content-addressed chunks and manifests to the same
`transactions_committed_*_v1` collection layout. A separate proxy reference
collection indexes namespace/root to manifest ID and retains import signatures.
Writes use majority reads and journaled majority writes, with the manifest
published only after every chunk is stored. Retried imports are idempotent.

This stage still returns the unavailable error after storage. It does not yet
verify Stark signatures or the computed commitment root, submit the root
transaction to C1, or forward datasets to C2. It must not be used as a
successful Paradex ingress until those steps are implemented.

The `Build image` GitHub Actions workflow publishes a versioned GHCR image
manually. The image is linked to this public repository so Kubernetes can pull
it anonymously.
