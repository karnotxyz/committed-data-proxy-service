# Committed data proxy service

Mainnet ingress for Paradex committed data. The first release exposes the
Madara JSON-RPC method on port 9945 and process health endpoints. It returns
an explicit `-32003` unavailable error for import requests until durable
storage and transaction routing are implemented. It never acknowledges an
import it has not stored.

The `Build image` GitHub Actions workflow publishes a versioned GHCR image
manually. The image is linked to this public repository so Kubernetes can pull
it anonymously.
