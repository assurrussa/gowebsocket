package websocketstream

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"runtime/debug"
	"strings"
	"time"

	libwebsocket "github.com/fasthttp/websocket"
	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/utils/v2"
	"github.com/valyala/fasthttp"
)

type Websocket interface {
	SetWriteDeadline(t time.Time) error
	NextWriter(messageType int) (io.WriteCloser, error)
	WriteMessage(messageType int, data []byte) error
	WriteControl(messageType int, data []byte, deadline time.Time) error

	SetPongHandler(h func(appData string) error)
	SetReadDeadline(t time.Time) error
	NextReader() (messageType int, r io.Reader, err error)

	Close() error
}

type Upgrader interface {
	Upgrade(w http.ResponseWriter, r *http.Request, responseHeader http.Header) (Websocket, error)
	UpgradeFastHTTP(ctx *fasthttp.RequestCtx, handler libwebsocket.FastHTTPHandler) error
}

//revive:disable:defer // recover is called within a deferred function elsewhere
func defaultRecover(c *libwebsocket.Conn) {
	if err := recover(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "panic: %v\n%s\n", err, debug.Stack())
		if err := c.WriteJSON(fiber.Map{"error": err}); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "could not write error response: %v\n", err)
		}
	}
}

type Config struct {
	// WriteBufferPool is a pool of buffers for write operations. If the value
	// is not set, then write buffers are allocated to the connection for the
	// lifetime of the connection.
	//
	// A pool is most useful when the application has a modest volume of writes
	// across a large number of connections.
	//
	// Applications should use a single pool for each unique value of
	// WriteBufferSize.
	WriteBufferPool libwebsocket.BufferPool

	// EnableCompression specifies if the client should attempt to negotiate
	// per message compression (RFC 7692). Setting this value to true does not
	// guarantee that compression will be supported. Currently only "no context
	// takeover" modes are supported.
	EnableCompression bool

	// RecoverHandler is a panic handler function that recovers from panics
	// Default recover function is used when nil and writes error message in a response field `error`
	// It prints stack trace to the stderr by default
	// Optional. Default: defaultRecover
	RecoverHandler func(*libwebsocket.Conn)
}

type upgraderImpl struct {
	upgrader               *libwebsocket.Upgrader
	upgraderFastHTTP       *libwebsocket.FastHTTPUpgrader
	recoverHandlerFastHTTP func(*libwebsocket.Conn)
}

func NewUpgrader(allowOrigins []string, secWsProtocols []string, cfgs ...Config) Upgrader {
	var cfg Config
	if len(cfgs) > 0 {
		cfg = cfgs[0]
	}

	if cfg.RecoverHandler == nil {
		cfg.RecoverHandler = defaultRecover
	}

	upgrader := &libwebsocket.Upgrader{
		HandshakeTimeout:  1 * time.Second,           // Slow clients should be oppressed.
		ReadBufferSize:    1024,                      // Max control frame payload size.
		WriteBufferSize:   1024,                      // To save memory on each connection, because app doesn't frame message.
		CheckOrigin:       checkOrigin(allowOrigins), // Simple check origin func.
		Subprotocols:      secWsProtocols,
		EnableCompression: cfg.EnableCompression,
		WriteBufferPool:   cfg.WriteBufferPool,
	}

	upgraderFastHTTP := &libwebsocket.FastHTTPUpgrader{
		HandshakeTimeout:  1 * time.Second,                   // Slow clients should be oppressed.
		ReadBufferSize:    1024,                              // Max control frame payload size.
		WriteBufferSize:   1024,                              // To save memory on each connection, because app doesn't frame message.
		CheckOrigin:       checkOriginFastHTTP(allowOrigins), // Simple check origin func.
		Subprotocols:      secWsProtocols,
		EnableCompression: cfg.EnableCompression,
		WriteBufferPool:   cfg.WriteBufferPool,
	}

	return &upgraderImpl{
		upgrader:               upgrader,
		upgraderFastHTTP:       upgraderFastHTTP,
		recoverHandlerFastHTTP: cfg.RecoverHandler,
	}
}

func (u *upgraderImpl) Upgrade(w http.ResponseWriter, r *http.Request, responseHeader http.Header) (Websocket, error) {
	return u.upgrader.Upgrade(w, r, responseHeader)
}

func (u *upgraderImpl) UpgradeFastHTTP(ctx *fasthttp.RequestCtx, handler libwebsocket.FastHTTPHandler) error {
	return u.upgraderFastHTTP.Upgrade(ctx, func(conn *libwebsocket.Conn) {
		defer u.recoverHandlerFastHTTP(conn)
		handler(conn)
	})
}

// IsWebSocketUpgrade returns true if the client requested upgrade to the WebSocket protocol.
func IsWebSocketUpgrade(c fiber.Ctx) bool {
	return libwebsocket.FastHTTPIsWebSocketUpgrade(c.RequestCtx())
}

// Based on echomdlwr.CORSWithConfig used in server/server.go.
func checkOriginFastHTTP(allowOrigins []string) func(fctx *fasthttp.RequestCtx) bool {
	allowOriginPatterns := make([]string, 0, len(allowOrigins))
	for _, origin := range allowOrigins {
		pattern := regexp.QuoteMeta(origin)
		pattern = strings.ReplaceAll(pattern, "\\*", ".*")
		pattern = strings.ReplaceAll(pattern, "\\?", ".")
		pattern = "^" + pattern + "$"
		allowOriginPatterns = append(allowOriginPatterns, pattern)
	}

	return func(fctx *fasthttp.RequestCtx) bool {
		origin := utils.UnsafeString(fctx.Request.Header.Peek("Origin"))
		if origin == "" {
			return false
		}

		for _, o := range allowOrigins { // Small O(N).
			if o == origin {
				return true
			}
		}

		// To avoid regex cost by invalid (long) domains (253 is domain name max limit).
		if len(origin) > (253+3+5) || !strings.Contains(origin, "://") {
			return false
		}

		for _, re := range allowOriginPatterns { // Small O(N).
			if match, _ := regexp.MatchString(re, origin); match {
				return true
			}
		}

		return false
	}
}

// Based on echomdlwr.CORSWithConfig used in server/server.go.
func checkOrigin(allowOrigins []string) func(*http.Request) bool {
	allowOriginPatterns := make([]string, 0, len(allowOrigins))
	for _, origin := range allowOrigins {
		pattern := regexp.QuoteMeta(origin)
		pattern = strings.ReplaceAll(pattern, "\\*", ".*")
		pattern = strings.ReplaceAll(pattern, "\\?", ".")
		pattern = "^" + pattern + "$"
		allowOriginPatterns = append(allowOriginPatterns, pattern)
	}

	return func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" {
			return false
		}

		for _, o := range allowOrigins { // Small O(N).
			if o == origin {
				return true
			}
		}

		// To avoid regex cost by invalid (long) domains (253 is domain name max limit).
		if len(origin) > (253+3+5) || !strings.Contains(origin, "://") {
			return false
		}

		for _, re := range allowOriginPatterns { // Small O(N).
			if match, _ := regexp.MatchString(re, origin); match {
				return true
			}
		}

		return false
	}
}
