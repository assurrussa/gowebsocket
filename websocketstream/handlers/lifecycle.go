package handlers

import (
	"context"
	"errors"
	"sync"
	"time"

	libwebsocket "github.com/fasthttp/websocket"

	"github.com/assurrussa/gowebsocket/eventstream"
	"github.com/assurrussa/gowebsocket/internal/safety"
	"github.com/assurrussa/gowebsocket/websocketstream"
)

// connection tracks admission across both synchronous and deferred upgrades.
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

// unavailable avoids running authentication after shutdown. Admission rechecks
// the state because authentication can overlap shutdown.
func (h *HTTPHandler) unavailable(ctx context.Context) bool {
	select {
	case <-h.shutdownCh:
		h.beginShutdown(ctx)
	default:
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.closed
}

// reserve owns only the immutable identity, never a host request/context. The
// handoff timer also bounds deferred Fiber callbacks and late custom upgrades.
func (h *HTTPHandler) reserve(uid eventstream.UserID) *connection {
	ctx, cancel := context.WithCancel(eventstream.WithUserID(context.Background(), uid))
	state := &connection{handler: h, ctx: ctx, cancel: cancel}
	if !h.admit(state) {
		cancel()
		h.rejected.Add(1)
		return nil
	}
	state.mu.Lock()
	if state.finished || state.ctx.Err() != nil {
		state.mu.Unlock()
		state.finish()
		h.rejected.Add(1)
		return nil
	}
	state.timer = time.AfterFunc(h.handoffTimeout, state.expirePending)
	state.mu.Unlock()
	return state
}

func (h *HTTPHandler) runConnection(state *connection, ws websocketstream.Websocket, uid eventstream.UserID) {
	if !state.attach(ws) {
		_ = ws.Close()
		return
	}
	defer state.finish()
	defer ws.Close()
	h.accepted.Add(1)
	err := safety.Call(func() error { return h.serveConnection(state.ctx, ws, uid) })
	if err != nil && closeCode(err) == libwebsocket.CloseInternalServerErr {
		h.logger.ErrorContext(state.ctx, "websocket connection failed", "panic", errors.Is(err, safety.ErrPanic))
	}
}
