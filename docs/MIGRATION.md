# Migration from b55c50e

The constructor names and ordinary calling conventions are retained. The
logging parameter now accepts a smaller structural interface implemented by
both `*slog.Logger` and existing `gologger.Logger`. Consumers assigning the
constructor itself to an explicitly typed function variable may need to update
that variable's parameter type.

## Intentional behavioral changes

- Close/Shutdown is terminal. Publish/Subscribe/Submit reject work after closure.
  Cancel individual subscriptions with their context; do not use Close as Wait.
- Input must validate and use text frames. Invalid, unknown and oversized input
  terminates the connection. Plain JSON is now the default input format.
- Legacy base64(JSON) endpoints must explicitly set WithWireFormat(handlers.LegacyBase64).
  There is no heuristic fallback that might reinterpret malformed input.
- Events must round-trip through JSON. Publish/Submit now take owned snapshots,
  not shared references to mutable application objects. Each recipient gets a
  fresh value. Put hidden application state outside the transport event.
- Processing defaults to one FIFO worker rather than unbounded concurrent
  goroutines. Configure WithWorkers explicitly when completion ordering is not
  required, and handle overload through Submit.
- Cancellation propagates on disconnect. Detached processing must be explicitly
  enabled on an injected processor with WithDetachedContext(true).
- Per-user/global subscriptions and messages/bytes are bounded. A slow consumer
  is disconnected; treat reconnect as a request to reload application state.
- Missing/invalid identity is HTTP 401 before upgrade. The legacy GetUUID helper
  remains, but WithUserIDExtractor is preferred. Verified identity is available
  through eventstream.UserIDFromContext inside the domain handler.
- Raw byte/string JSONEventWriter inputs are validated. StrictJSONEventWriter
  is additive and uses standard JSON behavior for byte slices and strings.

## Unreleased error-classification correction

Inbound callback panics and submissions to a closed processor now close with
1011 (server error) and increment InternalClosed, rather than 1008 and PolicyClosed.
Invalid client events still use 1008; overload and cancellation retain their
existing close codes. No callback error details are sent to the client.

## Unreleased net/http adapter (additive)

`*handlers.HTTPHandler` now implements `http.Handler` through
`ServeHTTP(http.ResponseWriter, *http.Request)`. Register it with
`mux.Handle("/ws", h)`. Existing constructors, `Serve(fiber.Ctx) error`,
`WithUserIDExtractor` and the legacy Fiber context-key path are unchanged.
Existing Fiber applications do not need to migrate.

For net/http, configure `WithNetHTTPUserIDExtractor` with a
`func(*http.Request) (eventstream.UserID, error)` that resolves a verified
application identity. A net/http-only handler can pass `nil, ""` for the
shutdown channel and legacy Fiber context key in `NewOptions`. The net/http
adapter requires its own explicit extractor: absence, an error, a zero ID or a
panic returns HTTP 401 before upgrade, with no Fiber or context-key fallback.
An extractor may read trusted middleware values from the request context, but
only the resolved immutable user ID survives into the connection context. HTTP
request cancellation and other request values are not the socket's lifecycle;
the handler cancels connection work on disconnect or shutdown.

One handler can serve both Fiber and net/http when both identity extractors are
configured. Both adapters use the same admission limit, connection pumps,
wire formats, origin policy, event processors, statistics and shutdown state.
Do not construct a second handler merely to mount another router unless
independent connection limits and lifecycles are intended.

Configure `http.Server` read-header/read/write/idle timeouts and preserve
`http.Hijacker` in response-writer middleware. Call `h.Shutdown(ctx)` explicitly:
`http.Server.Shutdown` does not close or wait for hijacked WebSockets. Check its
error before treating WebSocket shutdown as complete. The complete
[`examples/nethttp`](../examples/nethttp/main.go) demo binds to
`http://127.0.0.1:8081` and includes session extraction, an exact Origin allowlist,
JSON echo, HTTP timeouts and signal-driven shutdown. Its session is demo-only.

## Startup/shutdown order

Create stream, domain handlers and HTTPHandler. Register h.Serve as the Fiber
route or register h directly with a net/http mux. At shutdown stop admissions
using h.Shutdown(ctx), then close the stream
and caller-owned processors, then stop the HTTP listener. Reuse a deadline for
the shutdown operation, not the already-canceled signal context.

Injected streams/processors/loggers are borrowed. The handler closes only its
own default processor. A timeout cancels cooperative callbacks but cannot stop
an arbitrary blocked callback; do not interpret a timed-out Shutdown as complete.

## Release gate

Before merging, run the full matrix on Go 1.27.0 and 1.27.1, including real
FastHTTP/Fiber compressed input and bidirectional WebSocket tests. Run lint with
the repository's existing rules, and try a real application consumer. Local
stdlib-core tests on an older compiler do not replace that gate. No public
release or tag is created by this change.
