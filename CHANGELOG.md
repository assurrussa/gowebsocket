# Changelog

## Unreleased — review hardening

Bounded messages, decoded payloads, outbound writes, subscriber mailboxes and
worker queues. Fixed shutdown admission races, stale subscriber registries and
reader/writer teardown. Added context identity before upgrade, validation and
panic containment on input/callback boundaries. Replaced observer history with
owned JSON snapshots and made cancellation/ordering/overflow contracts explicit.

Added JSON wire mode and opt-in v1 envelopes without silently changing the
legacy base64 mode. Kept the legacy UUID extraction helper, upgraded origin
matching and preserved handshake status codes. Removed Conn pooling and retained
Fiber request metadata; added resource Stats and defined ownership of processors.

Added deterministic regression/integration tests, fuzz targets, benchmarks,
runnable example, consumer smoke checks and release documentation. See
[the migration guide](docs/MIGRATION.md) for intentional behavioral changes.
