package handlers

import (
	"context"
	"sync"
	"time"

	libwebsocket "github.com/fasthttp/websocket"

	"github.com/assurrussa/gowebsocket/websocketstream"
)

type wsCloser struct {
	once    sync.Once
	ws      websocketstream.Websocket
	timeout time.Duration
}

func newWsCloser(ws websocketstream.Websocket, timeout time.Duration) *wsCloser {
	return &wsCloser{ws: ws, timeout: timeout}
}

// Close sends one bounded close control frame, then interrupts I/O immediately.
// It does not sleep and does not promise a complete peer close handshake.
func (c *wsCloser) Close(ctx context.Context, code int) {
	c.once.Do(func() {
		defer c.ws.Close()
		deadline := time.Now().Add(c.timeout)
		if limit, ok := ctx.Deadline(); ok && limit.Before(deadline) {
			deadline = limit
		}
		if ctx.Err() == nil {
			_ = c.ws.WriteControl(libwebsocket.CloseMessage, libwebsocket.FormatCloseMessage(code, ""), deadline)
		}
	})
}
