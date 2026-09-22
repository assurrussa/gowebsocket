package handlers

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	logger "github.com/assurrussa/gologger"
	"github.com/assurrussa/goshared/pkg/sharedtypes"
	libwebsocket "github.com/fasthttp/websocket"
	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/utils/v2"
	"golang.org/x/sync/errgroup"

	eventstream "github.com/assurrussa/gowebsocket/eventstream"
	websocketstream "github.com/assurrussa/gowebsocket/websocketstream"
	eventadapter "github.com/assurrussa/gowebsocket/websocketstream/eventadapter"
	eventprocessor "github.com/assurrussa/gowebsocket/websocketstream/eventprocessor"
)

const (
	writeTimeout = time.Second
)

var decoding = base64.StdEncoding

type eventStream interface {
	Subscribe(ctx context.Context, userID sharedtypes.UserID) (<-chan eventstream.Event, error)
}

//go:generate options-gen -out-filename=handler_options.gen.go -from-struct=Options
type Options struct {
	pingPeriod time.Duration `default:"10s" validate:"omitempty,min=100ms,max=30s"`

	logger             logger.Logger            `option:"mandatory" validate:"required"`
	eventStream        eventStream              `option:"mandatory" validate:"required"`
	upgrader           websocketstream.Upgrader `option:"mandatory" validate:"required"`
	shutdownCh         <-chan struct{}          `option:"mandatory" validate:"required"`
	userIDCtxKey       string                   `option:"mandatory" validate:"required"`
	eventProcessors    map[string]eventprocessor.EventProcessor
	eventAdapters      map[string]eventadapter.EventAdapter
	readEventProcessor websocketstream.ReadEventProcessor
	eventWriter        websocketstream.EventWriter
	eventAdapter       websocketstream.EventAdapter
}

type HTTPHandler struct {
	Options
	pingPeriod time.Duration
	pongWait   time.Duration
}

// NewHTTPHandler creates a new WebSocket handler with FastHTTP optimization.
func NewHTTPHandler(opts Options) (*HTTPHandler, error) {
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("validate options: %w", err)
	}
	opts.logger = opts.logger.WithNamed("websocket")

	if opts.readEventProcessor == nil {
		p, err := eventprocessor.NewProcessor(eventprocessor.NewOptions(
			opts.logger,
			eventprocessor.WithProcessors(opts.eventProcessors),
			eventprocessor.WithMaxTimeWait(writeTimeout),
		))
		if err != nil {
			return nil, fmt.Errorf("create event processor: %w", err)
		}

		opts.readEventProcessor = p
	}

	if opts.eventWriter == nil {
		opts.eventWriter = websocketstream.JSONEventWriter{}
	}

	if opts.eventAdapter == nil {
		eventAdapter, err := eventadapter.NewAdapter(eventadapter.NewOptions(eventadapter.WithProcessors(opts.eventAdapters)))
		if err != nil {
			return nil, fmt.Errorf("create event adapter: %w", err)
		}
		opts.eventAdapter = eventAdapter
	}

	return &HTTPHandler{
		Options:    opts,
		pingPeriod: opts.pingPeriod,
		pongWait:   pongWait(opts.pingPeriod),
	}, nil
}

func (h *HTTPHandler) Serve(c fiber.Ctx) error {
	conn := acquireConn()
	// locals
	c.Request().VisitUserValues(func(key []byte, value any) {
		conn.locals[string(key)] = value
	})
	// params
	params := c.Route().Params
	for i := 0; i < len(params); i++ {
		conn.params[utils.CopyString(params[i])] = utils.CopyString(c.Params(params[i]))
	}
	// queries
	conn.queries = c.Queries()
	// cookies
	for key, value := range c.Request().Header.Cookies() {
		conn.cookies[string(key)] = string(value)
	}
	// headers
	conn.headers = c.GetReqHeaders()
	// ip address
	conn.ip = c.IP()

	if err := h.upgrader.UpgradeFastHTTP(c.RequestCtx(), func(ws *libwebsocket.Conn) {
		conn.Conn = ws
		defer releaseConn(conn)
		h.handleWebSocketConnection(conn)
	}); err != nil {
		h.logger.WarnContext(context.Background(), "failed to upgrade websocket connection", logger.Error(err))
		// Upgrading required
		return fiber.ErrUpgradeRequired
	}

	return nil
}

// handleWebSocketConnection handles the WebSocket connection logic.
func (h *HTTPHandler) handleWebSocketConnection(ws *Conn) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wsCloser := newWsCloser(h.logger, ws)
	uid, ok := h.getUserID(ws)
	if !ok {
		h.logger.ErrorContext(ctx, "failed to find user id")
		wsCloser.Close(ctx, libwebsocket.CloseInternalServerErr)
		return
	}

	events, err := h.eventStream.Subscribe(ctx, uid)
	if err != nil {
		h.logger.ErrorContext(ctx, "cannot subscribe for events", logger.Error(err))
		wsCloser.Close(ctx, libwebsocket.CloseInternalServerErr)
		return
	}

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		return h.writeLoop(ctx, ws, events)
	})

	eg.Go(func() error {
		return h.readLoop(ctx, ws)
	})

	eg.Go(func() error {
		select {
		case <-ctx.Done():
		case <-h.shutdownCh:
			wsCloser.Close(ctx, libwebsocket.CloseNormalClosure)
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		if !errors.Is(err, libwebsocket.ErrCloseSent) {
			h.logger.ErrorContext(ctx, "unexpected error", logger.Error(err))
			wsCloser.Close(ctx, libwebsocket.CloseInternalServerErr)
		}
		return
	}

	wsCloser.Close(ctx, libwebsocket.CloseNormalClosure)
}

// readLoop listen PONGs.
func (h *HTTPHandler) readLoop(ctx context.Context, ws websocketstream.Websocket) error {
	defer func() {
		h.logger.DebugContext(ctx, "ws read loop finished")
	}()
	h.logger.DebugContext(ctx, "ws read loop started")

	ws.SetPongHandler(func(string) error {
		h.logger.DebugContext(ctx, "pong")
		return ws.SetReadDeadline(time.Now().Add(h.pongWait))
	})

	if err := ws.SetReadDeadline(time.Now().Add(h.pongWait)); err != nil {
		return fmt.Errorf("set first read deadline: %w", err)
	}
	for {
		mt, r, err := ws.NextReader()
		if libwebsocket.IsCloseError(err, libwebsocket.CloseNormalClosure) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("get next reader: %w", err)
		}

		rawMessage, err := io.ReadAll(r)
		if err != nil {
			h.logger.ErrorContext(ctx, "can't read message", slog.Int("mt", mt), logger.Error(err))

			continue
		}

		message, err := decoding.DecodeString(string(rawMessage))
		if err != nil {
			h.logger.ErrorContext(ctx, "can't read decode", slog.Int("mt", mt), logger.Error(err))

			continue
		}

		event, err := h.eventAdapter.ReverseAdapt(message)
		if err != nil {
			h.logger.ErrorContext(ctx, "can't read message", slog.Int("mt", mt), logger.Error(err))

			continue
		}

		h.readEventProcessor.Process(ctx, event)
	}
}

// writeLoop listen events and writes them into Websocket.
func (h *HTTPHandler) writeLoop(ctx context.Context, ws websocketstream.Websocket, events <-chan eventstream.Event) error {
	defer func() {
		h.logger.DebugContext(ctx, "ws write loop finished")
	}()
	h.logger.DebugContext(ctx, "ws write loop started")

	pingTicker := time.NewTicker(h.pingPeriod)
	defer pingTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-pingTicker.C:
			if err := ws.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
				return fmt.Errorf("set write deadline: %w", err)
			}
			if err := ws.WriteMessage(libwebsocket.PingMessage, nil); err != nil {
				return fmt.Errorf("write ping message: %w", err)
			}
			h.logger.DebugContext(ctx, "ping")

		case event, ok := <-events:
			if !ok {
				return errors.New("events stream was closed")
			}

			if !ok {
				return nil
			}

			adapted, err := h.eventAdapter.Adapt(event)
			if err != nil {
				h.logger.ErrorContext(ctx, "cannot adapt event to out stream", logger.Error(err))
				continue
			}

			if err := ws.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
				return fmt.Errorf("set write deadline: %w", err)
			}

			wr, err := ws.NextWriter(libwebsocket.TextMessage)
			if err != nil {
				return fmt.Errorf("get next writer: %w", err)
			}

			if err := h.eventWriter.Write(adapted, wr); err != nil {
				return fmt.Errorf("write data to connection: %w", err)
			}

			if err := wr.Close(); err != nil {
				return fmt.Errorf("flush writer: %w", err)
			}
		}
	}
}

// getUserID retrieves the user ID from the fiber context.

// userWithUUID defines an interface for any struct that has a GetUUID method.
// This allows us to safely get the user ID from the session struct without import cycles.
type userWithUUID interface {
	GetUUID() sharedtypes.UserID
}

func (h *HTTPHandler) getUserID(ctx *Conn) (sharedtypes.UserID, bool) {
	val := ctx.Locals(h.userIDCtxKey)
	if val == nil {
		return sharedtypes.UserIDNil, false
	}

	if userID, ok := val.(sharedtypes.UserID); ok {
		return userID, !userID.IsZero()
	}

	if user, ok := val.(userWithUUID); ok && user != nil {
		userID := user.GetUUID()
		return userID, !userID.IsZero()
	}

	return sharedtypes.UserIDNil, false
}

func pongWait(ping time.Duration) time.Duration {
	return ping * 3 / 2
}
