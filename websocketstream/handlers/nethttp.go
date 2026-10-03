package handlers

import (
	"bufio"
	"errors"
	"net"
	"net/http"

	"github.com/assurrussa/gowebsocket/eventstream"
	"github.com/assurrussa/gowebsocket/internal/safety"
	"github.com/assurrussa/gowebsocket/websocketstream"
)

var _ http.Handler = (*HTTPHandler)(nil)

// ServeHTTP authenticates before upgrading and runs the shared connection
// lifecycle until the socket closes. Configure WithNetHTTPUserIDExtractor;
// Fiber identity callbacks and context keys are never used here. Only the
// resolved UserID survives upgrade, not request values or cancellation.
// Call Shutdown explicitly: http.Server.Shutdown does not close hijacked sockets.
func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.unavailable(r.Context()) {
		h.rejectHTTP(w, http.StatusServiceUnavailable)
		return
	}
	var uid eventstream.UserID
	err := safety.Call(func() error {
		if h.netHTTPUserIDExtractor == nil {
			return errors.New("net/http identity extractor is required")
		}
		var err error
		uid, err = h.netHTTPUserIDExtractor(r)
		return err
	})
	if err != nil || uid.IsZero() {
		h.rejectHTTP(w, http.StatusUnauthorized)
		return
	}
	state := h.reserve(uid)
	if state == nil {
		http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		return
	}
	defer state.finish()
	response := &upgradeResponse{ResponseWriter: w}
	var ws websocketstream.Websocket
	err = safety.Call(func() error {
		var err error
		ws, err = h.upgrader.Upgrade(response, r, nil)
		return err
	})
	if err != nil || safety.IsNil(ws) {
		h.rejected.Add(1)
		if !safety.IsNil(ws) {
			_ = ws.Close()
		}
		response.fail()
		return
	}
	h.runConnection(state, ws, uid)
}

func (h *HTTPHandler) rejectHTTP(w http.ResponseWriter, status int) {
	h.rejected.Add(1)
	http.Error(w, http.StatusText(status), status)
}

// Track committed responses and hijacks so a failed/panicking custom upgrader
// cannot cause a second HTTP response or leak a socket it already hijacked.
type upgradeResponse struct {
	http.ResponseWriter
	written bool
	conn    net.Conn
}

func (w *upgradeResponse) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *upgradeResponse) WriteHeader(status int) {
	w.ResponseWriter.WriteHeader(status)
	if status == http.StatusSwitchingProtocols || status >= http.StatusOK {
		w.written = true
	}
}

func (w *upgradeResponse) Write(data []byte) (int, error) {
	w.written = true
	return w.ResponseWriter.Write(data)
}

func (w *upgradeResponse) Flush() { _ = w.FlushError() }

func (w *upgradeResponse) FlushError() error {
	err := http.NewResponseController(w.ResponseWriter).Flush()
	if !errors.Is(err, http.ErrNotSupported) {
		// Flushing commits the implicit status, including when the network write fails.
		w.written = true
	}
	return err
}

func (w *upgradeResponse) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, rw, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err == nil {
		w.conn = conn
	}
	return conn, rw, err
}

func (w *upgradeResponse) fail() {
	if w.conn != nil {
		_ = w.conn.Close()
		return
	}
	if !w.written {
		http.Error(w, "websocket upgrade failed", http.StatusInternalServerError)
	}
}
