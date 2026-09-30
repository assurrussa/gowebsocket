# Contributing

Use Go 1.27.1. Keep compatibility changes explicit in docs/MIGRATION.md and add
regression tests for each fixed concurrency or transport failure.

`make check` must not modify tracked files. `make fix`, `make tidy` and
`make generate` are explicit maintenance operations. Review their diffs before
committing. Generation uses public mockgen v0.6.0; lint uses v2.14.0 and the
existing .golangci.yml policy. No globally installed private tools are required.

Tests must synchronize with channels/barriers rather than sleep to guess that a
server or worker has started. Bound test operations with deadlines. Run
`go test -race -count=5 ./...`, compile examples and run the clean consumer test.
Use `make bench-all` for local measurements and record toolchain, hardware,
payload and subscriber counts before making performance claims.

The transport is process-local and best-effort. Do not add distributed brokers,
replay or persistence without an explicit design and consumer requirement.
Do not weaken validation, suppress failed checks or call a timed-out shutdown
successful. An empty or unstarted check run is not a passing release gate.
