package handlers

import (
	"context"
	"sync"
	"time"

	"github.com/assurrussa/gowebsocket/websocketstream"
)

// connection tracks admission even before FastHTTP invokes its hijack callback.
type connection struct {
	mu      sync.Mutex
	handler *HTTPHandler
	//nolint:containedctx // Connection carries lifecycle context.
	ctx      context.Context
	cancel   context.CancelFunc
	ws       websocketstream.Websocket
	finished bool
	once     sync.Once
	timer    *time.Timer
}

func (c *connection) attach(ws websocketstream.Websocket) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished || c.ctx.Err() != nil {
		return false
	}
	c.ws = ws
	if c.timer != nil {
		c.timer.Stop()
	}
	return true
}

func (c *connection) finish() {
	c.once.Do(func() {
		c.cancel()
		c.mu.Lock()
		c.finished = true
		c.ws = nil
		if c.timer != nil {
			c.timer.Stop()
		}
		c.mu.Unlock()
		c.handler.mu.Lock()
		delete(c.handler.active, c)
		c.handler.mu.Unlock()
		c.handler.wg.Done()
	})
}

func (c *connection) expirePending() {
	c.mu.Lock()
	pending := c.ws == nil
	if pending {
		c.cancel()
	}
	c.mu.Unlock()
	if pending {
		c.finish()
	}
}

func (c *connection) stop() {
	c.cancel()
	c.mu.Lock()
	pending := c.ws == nil
	c.mu.Unlock()
	if pending {
		c.finish()
	}
	// Attached sockets are closed by the coordinator, with a bounded close frame.
}
