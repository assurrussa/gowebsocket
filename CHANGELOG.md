# Changelog

## Unreleased

Added `HTTPHandler.ServeHTTP` for standard-library `net/http` routers and
`WithNetHTTPUserIDExtractor` for explicit, verified identity before upgrade.
The Fiber API remains compatible. Both adapters share bounded connection
admission, pumps, processing, statistics and shutdown. Missing or invalid
net/http identity, including extractor panics, returns HTTP 401 before upgrade.
Only the resolved user ID is carried into the connection context.

Added a runnable loopback-only `examples/nethttp` JSON echo demo with a browser
client, exact Origin allowlist, demo session, HTTP timeouts and signal shutdown.
Documented explicit WebSocket shutdown because `http.Server.Shutdown` does not
close or wait for hijacked sockets. See [migration notes](docs/MIGRATION.md).

## v0.2.0 (2026-09-30)

Bounded messages, decoded payloads, outbound writes, subscriber mailboxes and
worker queues. Fixed shutdown admission races, stale subscriber registries and
reader/writer teardown. Added context identity before upgrade, validation and
panic containment on input/callback boundaries. Replaced observer history with
owned JSON snapshots and made cancellation/ordering/overflow contracts explicit.

Added plain JSON wire mode as default and opt-in v1 envelopes, with legacy base64
mode available via WithWireFormat(handlers.LegacyBase64). Kept the legacy UUID extraction helper,
upgraded origin matching and preserved handshake status codes. Removed Conn pooling and retained
Fiber request metadata; added resource Stats and defined ownership of processors.

Added deterministic regression/integration tests, fuzz targets, benchmarks,
runnable example, consumer smoke checks and release documentation. See
[the migration guide](docs/MIGRATION.md) for intentional behavioral changes.
