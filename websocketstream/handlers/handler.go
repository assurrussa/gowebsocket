// Package handlers integrates bounded realtime delivery with Fiber v3.
package handlers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	libwebsocket "github.com/fasthttp/websocket"
	"github.com/gofiber/fiber/v3"

	"github.com/assurrussa/gowebsocket/eventstream"
	"github.com/assurrussa/gowebsocket/internal/safety"
	"github.com/assurrussa/gowebsocket/internal/wire"
	"github.com/assurrussa/gowebsocket/websocketstream"
	"github.com/assurrussa/gowebsocket/websocketstream/eventadapter"
	"github.com/assurrussa/gowebsocket/websocketstream/eventprocessor"
)

var (
	errPolicy      = errors.New("invalid client event")
	errUnsupported = errors.New("only text WebSocket messages are supported")
)

type HTTPHandler struct {
	Options
	ownedProcessor                                                                     *eventprocessor.Processor
	mu                                                                                 sync.Mutex
	active                                                                             map[*connection]struct{}
	closed                                                                             bool
	wg                                                                                 sync.WaitGroup
	stop, done                                                                         chan struct{}
	accepted, rejected, incoming, outgoing, normalClosed, policyClosed, internalClosed atomic.Uint64
}

func NewHTTPHandler(opts Options) (*HTTPHandler, error) {
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("validate options: %w", err)
	}
	if opts.pongTimeout == 0 {
		opts.pongTimeout = opts.pingPeriod * 3
	}
	// Validate all callback registrations before starting owned workers.
	if opts.eventAdapter == nil {
		adapter, err := eventadapter.NewAdapter(eventadapter.NewOptions(eventadapter.WithProcessors(opts.eventAdapters)))
		if err != nil {
			return nil, err
		}
		opts.eventAdapter = adapter
	}
	if opts.eventWriter == nil {
		opts.eventWriter = websocketstream.JSONEventWriter{}
	}
	var owned *eventprocessor.Processor
	if opts.readEventProcessor == nil {
		processor, err := eventprocessor.NewProcessor(eventprocessor.NewOptions(opts.logger,
			eventprocessor.WithProcessors(opts.eventProcessors), eventprocessor.WithMaxTimeWait(opts.processTimeout),
			eventprocessor.WithMaxEventBytes(int(opts.maxDecodedBytes)),
			eventprocessor.WithQueueLimits(128, max(2<<20, int(opts.maxDecodedBytes)*4))))
		if err != nil {
			return nil, err
		}
		owned, opts.readEventProcessor = processor, processor
	}
	h := &HTTPHandler{
		Options: opts, ownedProcessor: owned, active: make(map[*connection]struct{}),
		stop: make(chan struct{}), done: make(chan struct{}),
	}
	if h.shutdownCh != nil {
		select {
		case <-h.shutdownCh:
			h.beginShutdown(context.Background())
		default:
			go func() {
				select {
				case <-h.stop:
				case <-h.shutdownCh:
					h.beginShutdown(context.Background())
				}
			}()
		}
	}
	return h, nil
}

// Serve authenticates before upgrading. It retains only the resolved immutable
// UserID, not Fiber's pooled request context or borrowed metadata.
func (h *HTTPHandler) Serve(c fiber.Ctx) error {
	select {
	case <-h.shutdownCh:
		h.beginShutdown(context.Background())
	default:
	}
	h.mu.Lock()
	closed := h.closed
	h.mu.Unlock()
	if closed {
		h.rejected.Add(1)
		return fiber.ErrServiceUnavailable
	}
	var uid eventstream.UserID
	err := safety.Call(func() error {
		if h.userIDExtractor != nil {
			var err error
			uid, err = h.userIDExtractor(c)
			return err
		}
		value := &Conn{locals: map[string]any{h.userIDCtxKey: c.Locals(h.userIDCtxKey)}}
		var ok bool
		uid, ok = h.getUserID(value)
		if !ok {
			return fiber.ErrUnauthorized
		}
		return nil
	})
	if err != nil || uid.IsZero() {
		h.rejected.Add(1)
		return fiber.ErrUnauthorized
	}
	ctx, cancel := context.WithCancel(eventstream.WithUserID(context.Background(), uid))
	state := &connection{handler: h, ctx: ctx, cancel: cancel}
	if !h.admit(state) {
		cancel()
		h.rejected.Add(1)
		return fiber.ErrServiceUnavailable
	}
	// A failed HTTP response may never invoke the hijack callback. Bound that
	// pending handoff as well; a late callback only closes its socket.
	state.mu.Lock()
	if state.finished || state.ctx.Err() != nil {
		state.mu.Unlock()
		state.finish()
		return fiber.ErrServiceUnavailable
	}
	state.timer = time.AfterFunc(h.handoffTimeout, state.expirePending)
	state.mu.Unlock()
	handedOff := false
	defer func() {
		if !handedOff {
			state.finish()
		}
	}()
	err = safety.Call(func() error {
		return h.upgrader.UpgradeFastHTTP(c.RequestCtx(), func(ws *libwebsocket.Conn) {
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
		})
	})
	if err != nil {
		h.rejected.Add(1)
		var handshake libwebsocket.HandshakeError
		if errors.As(err, &handshake) {
			return nil
		} // The upgrader already wrote 400/403/405.
		return fiber.NewError(fiber.StatusInternalServerError, "websocket upgrade failed")
	}
	handedOff = true
	return nil
}

func (h *HTTPHandler) admit(state *connection) bool {
	h.mu.Lock()
	// Recheck after authentication: it can overlap shutdown-channel closure.
	select {
	case <-h.shutdownCh:
		h.mu.Unlock()
		h.beginShutdown(state.ctx)
		return false
	default:
	}
	defer h.mu.Unlock()
	if h.closed || len(h.active) >= h.maxConnections {
		return false
	}
	h.active[state] = struct{}{}
	h.wg.Add(1)
	return true
}

func (h *HTTPHandler) serveConnection(ctx context.Context, ws websocketstream.Websocket, uid eventstream.UserID) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var events <-chan eventstream.Event
	err := safety.Call(func() error {
		var err error
		events, err = h.eventStream.Subscribe(ctx, uid)
		return err
	})
	if err == nil && events == nil {
		err = errors.New("event stream returned a nil subscription")
	}
	if err != nil {
		h.internalClosed.Add(1)
		newWsCloser(ws, h.closeTimeout).Close(context.WithoutCancel(ctx), libwebsocket.CloseInternalServerErr)
		return err
	}
	// Conn has no read-limit getter. Only adjust a known transport bound, so a
	// custom upgrader's existing limit is never accidentally increased. Unknown
	// transports still get the bounded wire/decoded reader in readLoop.
	if provider, ok := h.upgrader.(interface{ ReadLimit() int64 }); ok {
		if limit := provider.ReadLimit(); limit > 0 && h.maxMessageBytes < limit {
			if limiter, ok := ws.(interface{ SetReadLimit(limit int64) }); ok {
				limiter.SetReadLimit(h.maxMessageBytes)
			}
		}
	}
	results := make(chan error, 2)
	go func() { results <- safety.Call(func() error { return h.readLoop(ctx, ws) }) }()
	go func() { results <- safety.Call(func() error { return h.writeLoop(ctx, ws, events) }) }()
	remaining := 2
	select {
	case err = <-results:
		remaining--
	case <-ctx.Done():
	}
	cancel()
	code := closeCode(err)
	newWsCloser(ws, h.closeTimeout).Close(context.WithoutCancel(ctx), code)
	// Close above interrupts NextReader/NextWriter before waiting for the pumps.
	for i := 0; i < remaining; i++ {
		<-results
	}
	switch code {
	case libwebsocket.CloseNormalClosure:
		h.normalClosed.Add(1)
	case libwebsocket.CloseInternalServerErr:
		h.internalClosed.Add(1)
	default:
		h.policyClosed.Add(1)
	}
	return err
}

func closeCode(err error) int {
	switch {
	case err == nil, errors.Is(err, context.Canceled), errors.Is(err, libwebsocket.ErrCloseSent),
		libwebsocket.IsCloseError(err, libwebsocket.CloseNormalClosure, libwebsocket.CloseGoingAway):
		return libwebsocket.CloseNormalClosure
	case errors.Is(err, wire.ErrTooLarge), errors.Is(err, libwebsocket.ErrReadLimit):
		return libwebsocket.CloseMessageTooBig
	case errors.Is(err, eventprocessor.ErrOverloaded):
		return libwebsocket.CloseTryAgainLater
	case errors.Is(err, errUnsupported):
		return libwebsocket.CloseUnsupportedData
	case errors.Is(err, errPolicy), errors.Is(err, wire.ErrEncoding):
		return libwebsocket.ClosePolicyViolation
	default:
		return libwebsocket.CloseInternalServerErr
	}
}

func (h *HTTPHandler) readLoop(ctx context.Context, ws websocketstream.Websocket) error {
	ws.SetPongHandler(func(string) error { return ws.SetReadDeadline(time.Now().Add(h.pongTimeout)) })
	if err := ws.SetReadDeadline(time.Now().Add(h.pongTimeout)); err != nil {
		return err
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		kind, reader, err := ws.NextReader()
		if err != nil {
			return err
		}
		if kind != libwebsocket.TextMessage {
			return errUnsupported
		}
		message, err := wire.Decode(reader, h.wireFormat, h.maxMessageBytes, h.maxDecodedBytes)
		if err != nil {
			return err
		}
		event, err := h.eventAdapter.ReverseAdapt(message)
		if err != nil {
			return errPolicy
		}
		if err := eventstream.ValidateEvent(event); err != nil {
			return errPolicy
		}
		if submitter, ok := h.readEventProcessor.(websocketstream.EventSubmitter); ok {
			if err := submitter.Submit(ctx, event); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
					errors.Is(err, eventprocessor.ErrOverloaded) {
					return err
				}
				return errPolicy
			}
		} else {
			// Compatibility processors must return promptly; prefer EventSubmitter.
			h.readEventProcessor.Process(ctx, event)
		}
		h.incoming.Add(1)
	}
}

type limitedWriter struct {
	writer    io.Writer
	remaining int64
}

func (w *limitedWriter) Write(data []byte) (int, error) {
	if int64(len(data)) > w.remaining {
		return 0, wire.ErrTooLarge
	}
	n, err := w.writer.Write(data)
	w.remaining -= int64(n)
	if err == nil && n != len(data) {
		return n, io.ErrShortWrite
	}
	return n, err
}

func (h *HTTPHandler) writeLoop(ctx context.Context, ws websocketstream.Websocket, events <-chan eventstream.Event) error {
	ticker := time.NewTicker(h.pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := ws.SetWriteDeadline(time.Now().Add(h.writeTimeout)); err != nil {
				return err
			}
			if err := ws.WriteMessage(libwebsocket.PingMessage, nil); err != nil {
				return err
			}
		case event, ok := <-events:
			if !ok {
				return eventprocessor.ErrOverloaded
			} // Resync after terminated delivery.
			if err := eventstream.ValidateEvent(event); err != nil {
				return err
			}
			adapted, err := h.eventAdapter.Adapt(event)
			if err != nil {
				return err
			}
			if err := ws.SetWriteDeadline(time.Now().Add(h.writeTimeout)); err != nil {
				return err
			}
			writer, err := ws.NextWriter(libwebsocket.TextMessage)
			if err != nil {
				return err
			}
			err = safety.Call(func() error {
				return h.eventWriter.Write(adapted, &limitedWriter{writer: writer, remaining: h.maxOutboundBytes})
			})
			if err != nil {
				// Do not flush a partial application message as valid JSON.
				return err
			}
			if err := writer.Close(); err != nil {
				return err
			}
			h.outgoing.Add(1)
		}
	}
}

func (h *HTTPHandler) beginShutdown(ctx context.Context) {
	h.mu.Lock()
	if !h.closed {
		h.closed = true
		close(h.stop)
		states := make([]*connection, 0, len(h.active))
		for state := range h.active {
			states = append(states, state)
		}
		drainCtx := context.WithoutCancel(ctx)
		go func() {
			for _, state := range states {
				state.stop()
			}
			h.wg.Wait()
			if h.ownedProcessor != nil {
				_ = h.ownedProcessor.Shutdown(drainCtx)
			}
			close(h.done)
		}()
	}
	h.mu.Unlock()
}

// Shutdown closes active sockets, cancels their contexts and drains the owned
// default processor. Injected processors/streams/loggers remain caller-owned.
func (h *HTTPHandler) Shutdown(ctx context.Context) error {
	h.beginShutdown(ctx)
	select {
	case <-h.done:
		return nil
	default:
	}
	select {
	case <-h.done:
		return nil
	case <-ctx.Done():
		if h.ownedProcessor != nil {
			_ = h.ownedProcessor.Shutdown(ctx)
		}
		return ctx.Err()
	}
}

func (h *HTTPHandler) Close() error { return h.Shutdown(context.Background()) }

type Stats struct {
	Active                                                                             int
	Accepted, Rejected, Incoming, Outgoing, NormalClosed, PolicyClosed, InternalClosed uint64
}

func (h *HTTPHandler) Stats() Stats {
	h.mu.Lock()
	active := len(h.active)
	h.mu.Unlock()
	return Stats{
		Active: active, Accepted: h.accepted.Load(), Rejected: h.rejected.Load(),
		Incoming: h.incoming.Load(), Outgoing: h.outgoing.Load(), NormalClosed: h.normalClosed.Load(),
		PolicyClosed: h.policyClosed.Load(), InternalClosed: h.internalClosed.Load(),
	}
}
