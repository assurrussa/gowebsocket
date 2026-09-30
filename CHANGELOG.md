# Changelog

## Unreleased — review hardening

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
