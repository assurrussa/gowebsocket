# gowebsocket

Bounded, process-local realtime event delivery for Go applications using `net/http`
or Fiber v3/FastHTTP. Business code publishes an event to a user; all of that user's
active subscriptions receive independently decoded copies.

Requires **Go 1.27.0+**; the development toolchain remains **Go 1.27.1**.
This is not a durable broker, an authentication system or a distributed socket
cluster. There is no offline delivery, acknowledgement, replay or deduplication.

## Run a complete example

Fiber v3:

```sh
go run ./examples/basic
# Open http://127.0.0.1:8080
```

Standard-library `net/http`:

```sh
go run ./examples/nethttp
# Open http://127.0.0.1:8081
```

Both examples include a browser client, JSON input/output, a validated event,
a trusted identity extractor, an echo processor and shutdown. They bind only to
loopback. Their demo sessions are **not production authentication**.

For a consumer module:

```sh
go get github.com/assurrussa/gowebsocket@<reviewed-tag-or-commit>
```

The repository must be accessible to the caller. Publishing a public tag or
changing repository visibility is a separate maintainer decision.

## Wire compatibility

The handler defaults to **plain JSON input and output**.

To support legacy clients that wrap JSON messages in base64, opt in explicitly:

```go
handlers.WithWireFormat(handlers.LegacyBase64)
```

The default flat event must contain an `eventType` property; the `EventName()`
method does not add a JSON field automatically. Register the same name in
`WithEventAdapters` and `WithEventProcessors`. Events are validated before
business processing. Invalid JSON, unknown types, invalid events and binary
messages terminate the connection rather than being silently ignored.

An additive versioned envelope is available through
`eventadapter.NewEnvelopeEventProcessor[*YourEvent]()`. It produces
`{version:1,eventId,eventType,payload}` and checks that payload identity/type
match the envelope. The payload is the complete encoded application event.
Use the same envelope codec on both ends; this format is not auto-detected.

`JSONEventWriter` retains validated raw `[]byte`/`string` compatibility.
`StrictJSONEventWriter` uses ordinary `encoding/json` semantics; prefer
`json.RawMessage` when supplying pre-encoded JSON explicitly.

## Authentication and application authorization

Authenticate in your application, then return the verified `eventstream.UserID`
through `WithUserIDExtractor` for Fiber or `WithNetHTTPUserIDExtractor` for
`net/http`. Authentication happens **before** HTTP 101.
The processor retrieves the trusted value through:

```go
uid, ok := eventstream.UserIDFromContext(ctx)
```

Never authorize an operation using a `userId` received in the event body. Check
resource permissions in your domain handler. Expiring/revoking a login must also
close or reauthorize its WebSocket session in the host application.

The old `userIDCtxKey`/`GetUUID` path remains available for migration, including
custom UUID return types. An invalid `GetUUID()` never falls back to a session's
`String()` representation. This legacy path is Fiber-only. Neither adapter
retains the request, cookies or request-context values after the identity is
resolved. Only the immutable user ID is copied into the connection's context;
disconnect and handler shutdown cancel that context.

## Use with net/http

`*handlers.HTTPHandler` implements `http.Handler`. Register it directly with a
standard mux, using an explicit net/http identity extractor:

```go
h, err := handlers.NewHTTPHandler(handlers.NewOptions(
	logger, bus, upgrader, nil, "",
	handlers.WithNetHTTPUserIDExtractor(func(r *http.Request) (eventstream.UserID, error) {
		// Your application verifies a session/token or reads trusted middleware state.
		return authenticatedUserID(r)
	}),
	handlers.WithEventAdapters(adapters),
	handlers.WithEventProcessors(processors),
))
if err != nil {
	return err
}
mux := http.NewServeMux()
mux.Handle("/ws", h)
```

`authenticatedUserID`, `logger`, `bus`, `upgrader`, `adapters` and `processors`
above are application-owned. See [the runnable example](examples/nethttp/main.go)
for complete wiring. `NetHTTPUserIDExtractor` has signature
`func(*http.Request) (eventstream.UserID, error)`. A missing extractor, error,
zero ID or extractor panic rejects the request with HTTP 401 before upgrade.
There is no implicit request-context key lookup or fallback to Fiber identity.

The existing constructors, `Serve(fiber.Ctx) error` and Fiber identity options
remain available. Configure both extractors when serving both adapters through
one handler. They share connection admission, limits, processors, `Stats()` and
terminal `Shutdown(ctx)`; no separate WebSocket lifecycle is needed per router.
`net/http` middleware wrapping the response writer must preserve the HTTP
hijacking capability required by the upgrader.

## Resource limits and delivery guarantees

The handler defaults to a 96 KiB WebSocket message limit, a 64 KiB decoded input
limit, a 96 KiB output limit and 10,000 admitted connections. Reading is bounded
again after decompression, so a small compressed message cannot bypass the
application payload limit. Configure limits using `WithMessageLimits`,
`WithMaxOutboundBytes` and `WithMaxConnections`.

HTTPHandler tightens the built-in upgrader's `Config.ReadLimit` and never raises
it. To accept larger frames, increase both the upgrader and handler limits.
Custom upgraders can expose `ReadLimit() int64` for the same composition. Without
that optional provider, the handler preserves the existing transport limit and
enforces its own message and decoded-payload limits through bounded reading.

The in-memory stream defaults to 64 queued events / 1 MiB of encoded payload per
subscription, a 64 KiB event limit, 10,000 subscribers globally and 16 per user.
Use `inmem.NewWithConfig(inmem.DefaultConfig())` with adjusted values.
A slow subscriber is disconnected when either queue limit is exceeded. Healthy
subscribers continue receiving events. `ErrSlowConsumer` can therefore describe
partial fanout: retrying it blindly can duplicate delivery to healthy clients.
A terminated subscription should reconnect and reload current state through the
application's ordinary API.

Payloads are snapshotted as immutable JSON. Do not mutate an event concurrently
with Publish/Submit; after either returns, its queued snapshot is independent.
Each subscriber gets its own decoded event. EventID and EventName must remain
stable through the JSON round trip; a snapshot that changes either is rejected.
Unexported/JSON-ignored state must not be required for event validity. Non-JSON
events need an application adapter.
Byte accounting measures serialized queued payload, **not total heap/RSS**.
There can also be one decoded event in flight per subscriber, active worker
payloads, contexts and transport buffers. Custom JSON methods must be bounded,
deterministic and round-trip compatible. Size the global connection limit for
the available memory; finite defaults are not a capacity guarantee.

The processor uses one worker by default (global FIFO), a 128-event / 2 MiB queue
and a one-second per-task deadline. `WithWorkers(n)` enables parallel execution
and intentionally gives up completion ordering. `Submit` never waits for queue
capacity and returns `ErrOverloaded`; HTTPHandler observes this and closes the
client with 1013. Legacy `Process` remains but cannot report admission failure.
No goroutine is spawned per incoming message. Disconnected contexts cancel work
by default; `WithDetachedContext(true)` is an explicit opt-in for the old policy.

Callbacks must return promptly and honor cancellation where a context is
provided. A timeout cannot kill arbitrary Go code, and the library does not
attempt unsafe forced termination. Panics are contained at callback/pump/worker
boundaries without exposing panic values or message bodies to clients/logs.

## Lifecycle and ownership

Call `handler.Shutdown(ctx)` before shutting down the HTTP application, then
close the event stream and any **injected** processor you own. A closed shutdown
channel is a compatibility trigger for handler shutdown, including before the
first request. A nil channel means explicit shutdown only.

For `net/http`, `http.Server.Shutdown` does **not** close or wait for hijacked
WebSocket connections. Explicitly call `handler.Shutdown(ctx)` first, then close
caller-owned stream/processors and call `server.Shutdown(ctx)`. Use a fresh,
bounded context rather than the already-canceled signal context, and check all
shutdown errors. An HTTP server stopping is not proof its WebSockets stopped.

HTTPHandler owns its default processor and drains it automatically. It never
closes an injected processor, stream or logger. `Service.Shutdown` cancels all
subscriptions without requiring caller cancellation, clears registry entries
and rejects new work. `Processor.Shutdown` seals admission and drains accepted
work; on deadline it discards queued tasks and cancels active contexts. A later
successful Shutdown still waits for a non-cooperative callback to finish.
All shutdown operations are terminal and idempotent.

Connection pump completion cancels the sibling pump, sends one bounded close
control frame and closes the socket **before** waiting for blocked I/O. There
is no fixed sleep. This is bounded teardown, not a guaranteed complete peer
close handshake. Custom processors implementing only Process must return
promptly; use EventSubmitter to support observable nonblocking admission.

## Origins and operations

Use exact origin allowlists. An empty allowlist denies origins, and `*` is an
explicit all-HTTP(S)-origins opt-in, inappropriate for authenticated production
endpoints. Host globs are compiled once. `Config.AllowMissingOrigin` explicitly
supports non-browser clients; Origin itself is not authentication.
`NewUpgraderChecked` reports invalid configuration at startup. HTTP upgrade
errors retain their original status instead of becoming 426.

Configure HTTP deadlines in the host server as well. For `net/http`:

```go
server := &http.Server{
	Addr:              "127.0.0.1:8081",
	Handler:           mux,
	ReadHeaderTimeout: 5 * time.Second,
	ReadTimeout:       10 * time.Second,
	WriteTimeout:      10 * time.Second,
	IdleTimeout:       120 * time.Second,
}
```

For Fiber/FastHTTP:

```go
app := fiber.New(fiber.Config{
	ReadTimeout:  10 * time.Second,
	WriteTimeout: 10 * time.Second,
	IdleTimeout:  120 * time.Second,
})
```

FastHTTP's upgrader in the pinned version does not apply HandshakeTimeout to
the HTTP response write; the handler's handoff timeout releases its admission
slot but cannot interrupt a response still owned by the HTTP server.

`Stats()` snapshots are exposed by handler, stream and processor: active
connections/subscribers, queued payload, evictions, admission/processing counts,
worker duration and close categories. Export them through your own telemetry
system without user-ID labels. Snapshots are observational, not transactional.

## Development

```sh
make check       # read-only format/vet/lint/test/race/example/consumer gates
make fix         # explicitly mutating formatter
make generate    # pinned mockgen; no private toolsmocks command
make tidy        # explicitly mutating dependency maintenance
make bench-all
```

See [migration notes](docs/MIGRATION.md), [review coverage](docs/REVIEW.md),
[contributing](CONTRIBUTING.md) and [security policy](SECURITY.md).
