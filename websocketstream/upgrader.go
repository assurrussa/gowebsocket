package websocketstream

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	libwebsocket "github.com/fasthttp/websocket"
	"github.com/gofiber/fiber/v3"
	"github.com/valyala/fasthttp"

	"github.com/assurrussa/gowebsocket/internal/origin"
)

// Websocket deliberately keeps the legacy interface. HTTPHandler discovers
// SetReadLimit through an optional interface and always uses a bounded reader.
type Websocket interface {
	SetWriteDeadline(t time.Time) error
	NextWriter(messageType int) (io.WriteCloser, error)
	WriteMessage(messageType int, data []byte) error
	WriteControl(messageType int, data []byte, deadline time.Time) error
	SetPongHandler(h func(string) error)
	SetReadDeadline(t time.Time) error
	NextReader() (int, io.Reader, error)
	Close() error
}

type Upgrader interface {
	Upgrade(w http.ResponseWriter, r *http.Request, responseHeader http.Header) (Websocket, error)
	UpgradeFastHTTP(ctx *fasthttp.RequestCtx, handler libwebsocket.FastHTTPHandler) error
}

type Config struct {
	WriteBufferPool   libwebsocket.BufferPool
	EnableCompression bool
	// RecoverHandler is deferred around the FastHTTP callback only. HTTPHandler
	// independently contains panics in its pumps and application workers.
	RecoverHandler     func(*libwebsocket.Conn)
	AllowMissingOrigin bool
	HandshakeTimeout   time.Duration
	ReadLimit          int64
}

type upgraderImpl struct {
	upgrader               *libwebsocket.Upgrader
	upgraderFastHTTP       *libwebsocket.FastHTTPUpgrader
	recoverHandlerFastHTTP func(*libwebsocket.Conn)
	readLimit              int64
	initErr                error
}

// NewUpgrader preserves the legacy constructor. Invalid configuration fails
// closed during upgrade; use NewUpgraderChecked to report configuration errors
// at startup. A nil/empty allowlist denies origins; * is an explicit opt-in.
func NewUpgrader(allowed, protocols []string, configs ...Config) Upgrader {
	if len(configs) > 1 {
		return &upgraderImpl{initErr: errors.New("only one upgrader config is supported")}
	}
	var cfg Config
	if len(configs) == 1 {
		cfg = configs[0]
	}
	upgrader, err := NewUpgraderChecked(allowed, protocols, cfg)
	if err != nil {
		return &upgraderImpl{initErr: err}
	}
	return upgrader
}

func NewUpgraderChecked(allowed, protocols []string, cfg Config) (Upgrader, error) {
	if cfg.HandshakeTimeout == 0 {
		cfg.HandshakeTimeout = 5 * time.Second
	}
	if cfg.ReadLimit == 0 {
		cfg.ReadLimit = 96 << 10
	}
	if cfg.HandshakeTimeout < 0 || cfg.ReadLimit < 0 || cfg.ReadLimit > 64<<20 {
		return nil, errors.New("invalid upgrader limits")
	}
	matcher, err := origin.New(allowed, cfg.AllowMissingOrigin)
	if err != nil {
		return nil, err
	}
	if cfg.RecoverHandler == nil {
		cfg.RecoverHandler = defaultRecover
	}
	protocolCopy := append([]string(nil), protocols...)
	return &upgraderImpl{
		upgrader: &libwebsocket.Upgrader{
			HandshakeTimeout: cfg.HandshakeTimeout,
			ReadBufferSize:   1024, WriteBufferSize: 1024,
			CheckOrigin:  func(r *http.Request) bool { return matcher.Match(r.Header.Get("Origin")) },
			Subprotocols: protocolCopy, EnableCompression: cfg.EnableCompression, WriteBufferPool: cfg.WriteBufferPool,
		},
		upgraderFastHTTP: &libwebsocket.FastHTTPUpgrader{
			HandshakeTimeout: cfg.HandshakeTimeout,
			ReadBufferSize:   1024, WriteBufferSize: 1024,
			CheckOrigin: func(ctx *fasthttp.RequestCtx) bool {
				return matcher.Match(string(ctx.Request.Header.Peek("Origin")))
			},
			Subprotocols:      append([]string(nil), protocols...),
			EnableCompression: cfg.EnableCompression,
			WriteBufferPool:   cfg.WriteBufferPool,
		},
		recoverHandlerFastHTTP: cfg.RecoverHandler, readLimit: cfg.ReadLimit,
	}, nil
}

func defaultRecover(conn *libwebsocket.Conn) {
	// Do not write a JSON data frame concurrently with the data writer, and do
	// not return panic values (which may contain secrets) to the client.
	slog.Error("websocket callback panicked")
	_ = conn.WriteControl(libwebsocket.CloseMessage,
		libwebsocket.FormatCloseMessage(libwebsocket.CloseInternalServerErr, ""), time.Now().Add(time.Second))
	_ = conn.Close()
}

func (u *upgraderImpl) Upgrade(w http.ResponseWriter, r *http.Request, headers http.Header) (Websocket, error) {
	if u.initErr != nil {
		http.Error(w, "invalid websocket configuration", http.StatusInternalServerError)
		return nil, u.initErr
	}
	conn, err := u.upgrader.Upgrade(w, r, headers)
	if err != nil {
		return nil, err
	}
	conn.SetReadLimit(u.readLimit)
	return conn, nil
}

func (u *upgraderImpl) UpgradeFastHTTP(ctx *fasthttp.RequestCtx, handler libwebsocket.FastHTTPHandler) error {
	if u.initErr != nil {
		ctx.Error("invalid websocket configuration", fasthttp.StatusInternalServerError)
		return u.initErr
	}
	return u.upgraderFastHTTP.Upgrade(ctx, func(conn *libwebsocket.Conn) {
		defer conn.Close()
		defer func() {
			if recover() != nil {
				defaultRecover(conn)
			}
		}()
		defer func() {
			if recover() != nil {
				u.recoverHandlerFastHTTP(conn)
			}
		}()
		conn.SetReadLimit(u.readLimit)
		handler(conn)
	})
}

func IsWebSocketUpgrade(c fiber.Ctx) bool {
	return libwebsocket.FastHTTPIsWebSocketUpgrade(c.RequestCtx())
}
