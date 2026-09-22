package handlers

import (
	"context"
	"sync"
	"time"

	logger "github.com/assurrussa/gologger"
	libwebsocket "github.com/fasthttp/websocket"

	websocketstream "github.com/assurrussa/gowebsocket/websocketstream"
)

const (
	closeDeadline = 5 * time.Second
	graceTimeout  = 1 * time.Second
)

type wsCloser struct {
	once   sync.Once
	logger logger.Logger
	ws     websocketstream.Websocket
}

func newWsCloser(logger logger.Logger, ws websocketstream.Websocket) *wsCloser {
	return &wsCloser{
		ws:     ws,
		logger: logger,
		once:   sync.Once{},
	}
}

func (c *wsCloser) Close(ctx context.Context, code int) {
	c.once.Do(func() {
		c.logger.DebugContext(ctx, "close connection")

		_ = c.ws.WriteControl(
			libwebsocket.CloseMessage,
			libwebsocket.FormatCloseMessage(code, ""),
			time.Now().Add(closeDeadline),
		)

		time.Sleep(graceTimeout)
		_ = c.ws.Close()
	})
}
