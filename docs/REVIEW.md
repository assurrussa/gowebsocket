# Review implementation map

Base: b55c50ebbad25dc7b0734061d02d02847a068855. This map describes implementation
and regression coverage, not an assertion that every CI gate has passed.

## Runtime safety: R01–R11

R01: `internal/wire` limits both decompressed messages and decoded payloads;
HTTPHandler also tightens known transport read limits. Covered by wire limits and the
real WebSocket compressed-message test.

R02: shared ValidateEvent, typed-nil protection, generic Go-type checks and
adapter metadata validation. Invalid events never enter domain processing.

R03: identity resolved before upgrade; verified UserID in a typed context key.
Tests distinguish trusted identity from a conflicting client-supplied userId.

R04–R05: fixed workers and a message/byte-bounded FIFO, nonblocking admission,
queue-seal synchronization and deadline-aware shutdown. Core tests exercise
concurrent admission/shutdown, overload, cancellation, ordering and panics.

R06–R08: bounded subscriber mailboxes replace observer history. Immutable JSON
snapshots and independent decoded values define ownership. Terminal shutdown
cancels subscribers; churn and concurrent shutdown tests verify registry cleanup.
Overflow evicts only the slow consumer and explicitly reports partial fanout.

R09: either pump finishing cancels the connection and closes I/O before waiting
for its sibling. Pending hijacks have bounded admission lifetimes. Integration
tests cover normal close, failed upgrade and writer failure with a blocked reader.

R10–R11: panics are isolated in their actual worker/pump boundaries, without
payload disclosure. Constructors validate positive timeouts, ping/pong bounds,
limits and callback registrations. Zero ping is rejected, never passed to Ticker.

## Public contract and operations: R12–R22

R12–R14: explicit legacy/JSON wire formats, an opt-in versioned envelope,
principal extractor and preserved UUID/GetUUID compatibility path.

R15: unknown-event and malformed-input errors no longer echo message bodies.
Domain error/panic values are not included in built-in log messages.

R16–R17: original handshake status preserved; one precompiled fail-closed origin
matcher, exact origins/host globs and explicit missing-Origin policy.

R18–R19: independent write/process/pong/close/handoff settings; registry maps are
copied. Default processor ownership is explicit; injected resources are borrowed.

R20: the unmeasured Conn pool and retained request metadata are removed. Only
resolved UserID survives request handling. Failed/pending upgrades release their
admission state, and late callbacks close their socket without starting pumps.

R21–R22: local best-effort/no-replay semantics and payload budgets are documented.
Handler/stream/processor Stats expose connection, queue, overflow, processing,
duration and close counters without imposing a metrics backend.

## Packaging: R23–R27

R23: working localhost server/browser example uses the actual constructors and
an explicit JSON endpoint. The example has no real auth.

R24: check is non-mutating; fix/tidy/generate are separate. Mockgen and lint
versions are pinned; the private toolsmocks command and options-gen runtime are
removed from production code. Existing lint policy is retained, not weakened.

R25: transport/processor/stream leak checks, inbound WebSocket tests, origin and
auth rejections, immutable fanout, adversarial shutdown tests, fuzz targets and
microbenchmarks. Historical UUID/adapter/transport tests remain except the stream
and processor suites replaced with deterministic tests for the new contracts.

R26: proposed MIT license, SECURITY, CONTRIBUTING, migration/changelog,
clean consumer smoke test. Repository
visibility, release tags and protections are unchanged.

R27: standard JSON in production, a small slog-compatible logger interface and
handwritten options. gologger/go-json remain test dependencies where historical
tests use them. Broker, durability, rooms and presence are deliberately not added.

## Verification limitations

Full Go 1.27.1/Fiber integration and lint need the matching toolchain and network
access. Local verification and the PR's current SHA remain the release authority.

## Follow-up branch review

Review baseline: `fix/review-hardening` at `f443347`, compared with `b55c50e`.
The follow-up fixes preserve public signatures and the existing wire modes:

- Shutdown-channel monitoring starts at construction. An already closed channel
  rejects the first request, and admission rechecks the signal after identity
  extraction. Tests cover shutdown without requests and pending handoff expiry.
- RecoverHandler is deferred directly, preserving its ability to call recover
  on the original callback panic. Tests cover ordinary return, custom recovery
  and fallback recovery for handlers that panic or do not recover.
- Handler and built-in upgrader read limits compose using the smaller positive
  bound. Unknown custom-upgrader limits are preserved, while bounded payload
  decoding remains mandatory. This addresses PR #2's stricter-limit requirement.
- JSON snapshots check EventID as well as EventName. A disappearing identifier
  is rejected before publication or worker admission; zero IDs are not newly
  prohibited when the application contract permits them.
- Closed Publish/Submit calls reject before event validation or serialization.
  Admission still rechecks closure after snapshot construction, preserving the
  existing concurrent-close guarantee.
- Subscription errors, nil channels and panics send close 1011 and increment the
  internal-close counter. Real WebSocket tests also cover malformed/unknown input,
  binary frames, processor overload (1013) and outbound size rejection (1009).

HTTP response deadlines remain a host responsibility: FastHTTP's pinned
upgrader does not implement its HandshakeTimeout field. Handoff expiration bounds
handler admission state rather than terminating an HTTP-server response write.

Validation on Go 1.27.1 (darwin/arm64): `make check` passed with temporary Go/lint
caches. This ran format checks, `go vet ./...`, golangci-lint v2.14.0 (zero issues),
`go test -timeout=3m ./...`, `go test -race -count=5 -timeout=5m ./...`, example
builds and `scripts/test-consumer.sh`. The consumer probe uses a local replacement
of this checkout; it does not verify a published release. Go 1.27.0, production
load and deployment were not verified in this follow-up.
