# gowebsocket

Realtime WebSocket streaming and in-process event bus for Go applications on top of Fiber v3 and FastHTTP.

Requires **Go 1.27+**. Recommended toolchain: **Go 1.27.1**.

## Packages

- `github.com/assurrussa/gowebsocket/eventstream`: Event and EventStream interfaces, generated mocks.
- `github.com/assurrussa/gowebsocket/eventstream/inmem`: In-memory thread-safe event bus keyed by UserID based on observer properties.
- `github.com/assurrussa/gowebsocket/websocketstream`: WebSocket connection interfaces and event models.
- `github.com/assurrussa/gowebsocket/websocketstream/eventadapter`: Adapter bridging EventStream with WebSocket event processor.
- `github.com/assurrussa/gowebsocket/websocketstream/eventprocessor`: Process and route incoming/outgoing events.
- `github.com/assurrussa/gowebsocket/websocketstream/handlers`: Fiber v3 / FastHTTP websocket connection handlers, ping/pong loop, connection pool, and graceful closer.

## Installation

```sh
go get github.com/assurrussa/gowebsocket@latest
```

## Quick Start

```go
import (
    "github.com/assurrussa/gowebsocket/eventstream/inmem"
    "github.com/assurrussa/gowebsocket/websocketstream"
    "github.com/assurrussa/gowebsocket/websocketstream/eventadapter"
    "github.com/assurrussa/gowebsocket/websocketstream/eventprocessor"
    "github.com/assurrussa/gowebsocket/websocketstream/handlers"
)

// 1. Initialize event stream
es := inmem.New()

// 2. Initialize event processor and adapter
proc := eventprocessor.New(eventprocessor.NewOptions(log))
adapter := eventadapter.New(eventadapter.NewOptions(es, proc, log))

// 3. Register WebSocket route in Fiber
app.Get("/ws", handlers.New(handlers.NewOptions(
    upgrader,
    adapter,
    proc,
    log,
)).Handle())
```

## Development

- Full check: `make check`
- Tests: `go test ./...`
- Lint: `golangci-lint run ./...`
