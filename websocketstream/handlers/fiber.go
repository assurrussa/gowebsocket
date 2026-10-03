package handlers

import (
	"context"
	"errors"

	libwebsocket "github.com/fasthttp/websocket"
	"github.com/gofiber/fiber/v3"

	"github.com/assurrussa/gowebsocket/eventstream"
	"github.com/assurrussa/gowebsocket/internal/safety"
)

// Serve authenticates before upgrading. It retains only the resolved immutable
// UserID, not Fiber's pooled request context or borrowed metadata.
func (h *HTTPHandler) Serve(c fiber.Ctx) error {
	if h.unavailable(context.Background()) {
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
	state := h.reserve(uid)
	if state == nil {
		return fiber.ErrServiceUnavailable
	}
	handedOff := false
	defer func() {
		if !handedOff {
			state.finish()
		}
	}()
	err = safety.Call(func() error {
		return h.upgrader.UpgradeFastHTTP(c.RequestCtx(), func(ws *libwebsocket.Conn) {
			h.runConnection(state, ws, uid)
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
