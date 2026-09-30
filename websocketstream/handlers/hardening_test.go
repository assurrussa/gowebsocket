package handlers_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	libwebsocket "github.com/fasthttp/websocket"
	"github.com/gofiber/fiber/v3"
	"github.com/valyala/fasthttp/fasthttputil"
	"go.uber.org/goleak"

	"github.com/assurrussa/gowebsocket/eventstream"
	inmem "github.com/assurrussa/gowebsocket/eventstream/inmem"
	"github.com/assurrussa/gowebsocket/internal/testevent"
	"github.com/assurrussa/gowebsocket/websocketstream"
	"github.com/assurrussa/gowebsocket/websocketstream/eventadapter"
	"github.com/assurrussa/gowebsocket/websocketstream/eventprocessor"
	"github.com/assurrussa/gowebsocket/websocketstream/handlers"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreAnyFunction("github.com/valyala/fasthttp.(*workerPool).Start.func2"),
		goleak.IgnoreAnyFunction("github.com/valyala/fasthttp.updateServerDate.func1"),
	)
}

type checkedEvent struct {
	trusted, claimed eventstream.UserID
	body             string
}
type checkedProcessor struct{ output chan checkedEvent }

func (p checkedProcessor) Handle(ctx context.Context, event eventstream.Event) error {
	uid, ok := eventstream.UserIDFromContext(ctx)
	if !ok {
		return errors.New("missing trusted identity")
	}
	typed, ok := event.(*testevent.Event)
	if !ok {
		return errors.New("unexpected type")
	}
	select {
	case p.output <- checkedEvent{trusted: uid, claimed: typed.UserID, body: typed.Body}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type readyStream struct {
	*inmem.Service
	ready chan struct{}
	once  sync.Once
}

func (s *readyStream) Subscribe(ctx context.Context, id eventstream.UserID) (<-chan eventstream.Event, error) {
	events, err := s.Service.Subscribe(ctx, id)
	if err == nil {
		s.once.Do(func() { close(s.ready) })
	}
	return events, err
}

type testServer struct {
	h         *handlers.HTTPHandler
	bus       *readyStream
	uid       eventstream.UserID
	dialer    *libwebsocket.Dialer
	processed chan checkedEvent
}

func newHardeningServer(t *testing.T, options ...handlers.OptOptionsSetter) *testServer {
	t.Helper()
	s := &testServer{uid: eventstream.NewUserID(), processed: make(chan checkedEvent, 16), bus: &readyStream{Service: inmem.New(), ready: make(chan struct{})}}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	defaults := []handlers.OptOptionsSetter{
		handlers.WithEventAdapters(map[string]eventadapter.EventAdapter{"test": eventadapter.NewEventProcessor[*testevent.Event]()}),
		handlers.WithEventProcessors(map[string]eventprocessor.EventProcessor{"test": checkedProcessor{output: s.processed}}),
	}
	defaults = append(defaults, options...)
	var err error
	s.h, err = handlers.NewHTTPHandler(handlers.NewOptions(log, s.bus,
		websocketstream.NewUpgrader([]string{"http://localhost"}, nil, websocketstream.Config{EnableCompression: true}), nil, "uid", defaults...))
	if err != nil {
		t.Fatal(err)
	}
	app := fiber.New()
	app.Get("/ws", func(c fiber.Ctx) error { c.Locals("uid", s.uid); return s.h.Serve(c) })
	listener := fasthttputil.NewInmemoryListener()
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		_ = app.Listener(listener, fiber.ListenConfig{DisableStartupMessage: true})
	}()
	s.dialer = &libwebsocket.Dialer{NetDial: func(string, string) (net.Conn, error) { return listener.Dial() }, HandshakeTimeout: 2 * time.Second, EnableCompression: true}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := s.h.Shutdown(ctx); err != nil {
			t.Error(err)
		}
		if err := s.bus.Close(); err != nil {
			t.Error(err)
		}
		_ = app.Shutdown()
		_ = listener.Close()
		select {
		case <-exited:
		case <-ctx.Done():
			t.Error("listener did not exit")
		}
	})
	return s
}

func (s *testServer) dial(t *testing.T) *libwebsocket.Conn {
	t.Helper()
	header := http.Header{"Origin": []string{"http://localhost"}}
	conn, response, err := s.dialer.Dial("ws://localhost/ws", header)
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return conn
}

func TestInboundJSONAndTrustedIdentity(t *testing.T) {
	s := newHardeningServer(t, handlers.WithWireFormat(handlers.JSON))
	conn := s.dial(t)
	event := testevent.New("client message")
	event.UserID = eventstream.NewUserID()
	if err := conn.WriteJSON(event); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-s.processed:
		if got.trusted != s.uid || got.claimed != event.UserID || got.body != event.Body {
			t.Fatal("identity/message mismatch", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("incoming handler was not called")
	}
}

func TestLegacyBase64StillWorks(t *testing.T) {
	s := newHardeningServer(t)
	conn := s.dial(t)
	encoded, err := json.Marshal(testevent.New("legacy"))
	if err != nil {
		t.Fatal(err)
	}
	message := base64.StdEncoding.EncodeToString(encoded)
	if err := conn.WriteMessage(libwebsocket.TextMessage, []byte(message)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.processed:
	case <-time.After(2 * time.Second):
		t.Fatal("legacy format no longer works")
	}
}

func TestInvalidEventIsRejectedBeforeHandler(t *testing.T) {
	s := newHardeningServer(t, handlers.WithWireFormat(handlers.JSON))
	conn := s.dial(t)
	if err := conn.WriteMessage(libwebsocket.TextMessage, []byte(`{"eventType":"test"}`)); err != nil {
		t.Fatal(err)
	}
	_, _, err := conn.ReadMessage()
	if !libwebsocket.IsCloseError(err, libwebsocket.ClosePolicyViolation) {
		t.Fatalf("expected policy close: %v", err)
	}
	select {
	case <-s.processed:
		t.Fatal("invalid event reached handler")
	default:
	}
}

func TestCompressedMessageLimit(t *testing.T) {
	s := newHardeningServer(t, handlers.WithWireFormat(handlers.JSON), handlers.WithMessageLimits(2048, 1024))
	conn := s.dial(t)
	conn.EnableWriteCompression(true)
	if err := conn.WriteJSON(testevent.New(strings.Repeat("x", 10000))); err != nil {
		t.Fatal(err)
	}
	_, _, err := conn.ReadMessage()
	if !libwebsocket.IsCloseError(err, libwebsocket.CloseMessageTooBig) {
		t.Fatalf("expected oversized close: %v", err)
	}
	select {
	case <-s.processed:
		t.Fatal("oversized event reached handler")
	default:
	}
}

func TestIdentityRejectedBeforeUpgrade(t *testing.T) {
	s := newHardeningServer(t, handlers.WithUserIDExtractor(func(fiber.Ctx) (eventstream.UserID, error) {
		return eventstream.UserIDNil, errors.New("invalid session")
	}))
	conn, response, err := s.dialer.Dial("ws://localhost/ws", http.Header{"Origin": []string{"http://localhost"}})
	if conn != nil {
		_ = conn.Close()
	}
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected HTTP 401 before 101: %v %v", response, err)
	}
	if s.bus.Stats().Subscribers != 0 {
		t.Fatal("unauthorized subscription")
	}
}

func TestOriginStatusPreserved(t *testing.T) {
	s := newHardeningServer(t)
	conn, response, err := s.dialer.Dial("ws://localhost/ws", http.Header{"Origin": []string{"https://not-allowed.example"}})
	if conn != nil {
		_ = conn.Close()
	}
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("expected original HTTP 403: %v %v", response, err)
	}
	if s.h.Stats().Active != 0 {
		t.Fatal("failed upgrade leaked admission")
	}
}

type failingWriter struct{}

func (failingWriter) Write(any, io.Writer) error { return errors.New("intentional writer failure") }

func TestWriterFailureUnblocksReader(t *testing.T) {
	s := newHardeningServer(t, handlers.WithEventWriter(failingWriter{}))
	conn := s.dial(t)
	select {
	case <-s.bus.ready:
	case <-time.After(time.Second):
		t.Fatal("no subscription")
	}
	if err := s.bus.Publish(context.Background(), s.uid, testevent.New("outgoing")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("writer failure did not close connection")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.h.Shutdown(ctx); err != nil {
		t.Fatalf("blocked reader survived writer error: %v", err)
	}
	if s.h.Stats().Active != 0 {
		t.Fatal("active connection retained")
	}
}

func TestNormalCloseAndConnectionLimit(t *testing.T) {
	s := newHardeningServer(t, handlers.WithMaxConnections(1))
	conn := s.dial(t)
	extra, response, err := s.dialer.Dial("ws://localhost/ws", http.Header{"Origin": []string{"http://localhost"}})
	if extra != nil {
		_ = extra.Close()
	}
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("connection limit not enforced: %v %v", response, err)
	}
	if err := conn.WriteControl(libwebsocket.CloseMessage, libwebsocket.FormatCloseMessage(libwebsocket.CloseNormalClosure, ""), time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	_, _, err = conn.ReadMessage()
	if !libwebsocket.IsCloseError(err, libwebsocket.CloseNormalClosure) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.h.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestZeroPingRejected(t *testing.T) {
	bus := inmem.New()
	defer bus.Close()
	_, err := handlers.NewHTTPHandler(handlers.NewOptions(slog.Default(), bus,
		websocketstream.NewUpgrader([]string{"http://localhost"}, nil), nil, "uid", handlers.WithPingPeriod(0)))
	if err == nil {
		t.Fatal("zero ping accepted")
	}
}
