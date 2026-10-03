package handlers_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	libwebsocket "github.com/fasthttp/websocket"
	"github.com/gofiber/fiber/v3"
	"github.com/valyala/fasthttp"
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
	ready     chan struct{}
	once      sync.Once
	subscribe func(context.Context, eventstream.UserID) (<-chan eventstream.Event, error)
}

func (s *readyStream) Subscribe(ctx context.Context, id eventstream.UserID) (<-chan eventstream.Event, error) {
	if s.subscribe != nil {
		return s.subscribe(ctx, id)
	}
	events, err := s.Service.Subscribe(ctx, id)
	if err == nil {
		s.once.Do(func() { close(s.ready) })
	}
	return events, err
}

const (
	testOrigin   = "http://localhost"
	originHeader = "Origin"
)

type testServer struct {
	h         *handlers.HTTPHandler
	bus       *readyStream
	uid       eventstream.UserID
	dialer    *libwebsocket.Dialer
	processed chan checkedEvent
}

func newHardeningServer(t *testing.T, options ...handlers.OptOptionsSetter) *testServer {
	t.Helper()
	return newHardeningServerWithShutdown(t, nil, options...)
}

func newHardeningServerWithShutdown(t *testing.T, shutdown <-chan struct{}, options ...handlers.OptOptionsSetter) *testServer {
	t.Helper()
	upgrader := websocketstream.NewUpgrader([]string{testOrigin}, nil, websocketstream.Config{EnableCompression: true})
	return newHardeningServerWithUpgrader(t, shutdown, upgrader, options...)
}

func newHardeningServerWithUpgrader(
	t *testing.T, shutdown <-chan struct{}, upgrader websocketstream.Upgrader, options ...handlers.OptOptionsSetter,
) *testServer {
	t.Helper()
	s := &testServer{
		uid:       eventstream.NewUserID(),
		processed: make(chan checkedEvent, 16),
		bus:       &readyStream{Service: inmem.New(), ready: make(chan struct{})},
	}
	log := slog.New(slog.DiscardHandler)
	defaults := make([]handlers.OptOptionsSetter, 0, 2+len(options))
	defaults = append(defaults,
		handlers.WithEventAdapters(map[string]eventadapter.EventAdapter{
			testEventType: eventadapter.NewEventProcessor[*testevent.Event](),
		}),
		handlers.WithEventProcessors(map[string]eventprocessor.EventProcessor{testEventType: checkedProcessor{output: s.processed}}),
	)
	defaults = append(defaults, options...)
	var err error
	s.h, err = handlers.NewHTTPHandler(handlers.NewOptions(log, s.bus, upgrader, shutdown, "uid", defaults...))
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
	s.dialer = &libwebsocket.Dialer{
		NetDial:           func(string, string) (net.Conn, error) { return listener.Dial() },
		HandshakeTimeout:  2 * time.Second,
		EnableCompression: true,
	}
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
	header := http.Header{originHeader: []string{testOrigin}}
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
	forTransports(t, func(t *testing.T, newServer serverFactory) {
		t.Helper()
		s := newServer(t)
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
	})
}

func TestLegacyBase64StillWorks(t *testing.T) {
	forTransports(t, func(t *testing.T, newServer serverFactory) {
		t.Helper()
		s := newServer(t, handlers.WithWireFormat(handlers.LegacyBase64))
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
	})
}

func TestInvalidEventIsRejectedBeforeHandler(t *testing.T) {
	forTransports(t, func(t *testing.T, newServer serverFactory) {
		t.Helper()
		s := newServer(t, handlers.WithWireFormat(handlers.JSON))
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
	})
}

func TestCompressedMessageLimit(t *testing.T) {
	forTransports(t, func(t *testing.T, newServer serverFactory) {
		t.Helper()
		s := newServer(t, handlers.WithWireFormat(handlers.JSON), handlers.WithMessageLimits(2048, 1024))
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
	})
}

// Embedding the legacy interface hides optional transport limit information.
type unknownReadLimitUpgrader struct{ websocketstream.Upgrader }

func expectMessageTooBig(t *testing.T, conn *libwebsocket.Conn) {
	t.Helper()
	_, _, err := conn.ReadMessage()
	if !libwebsocket.IsCloseError(err, libwebsocket.CloseMessageTooBig) {
		t.Fatalf("expected oversized close: %v", err)
	}
}

func TestTransportReadLimitPreserved(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		for _, format := range []handlers.WireFormat{handlers.JSON, handlers.LegacyBase64} {
			name := fmt.Sprintf("unknown=%t/format=%s", unknown, format)
			t.Run(name, func(t *testing.T) {
				upgrader, err := websocketstream.NewUpgraderChecked([]string{testOrigin}, nil, websocketstream.Config{
					ReadLimit: 1024, EnableCompression: true,
				})
				if err != nil {
					t.Fatal(err)
				}
				if unknown {
					upgrader = unknownReadLimitUpgrader{upgrader}
				}
				s := newHardeningServerWithUpgrader(t, nil, upgrader, handlers.WithWireFormat(format))
				conn := s.dial(t)
				conn.EnableWriteCompression(false)
				payload, err := json.Marshal(testevent.New(strings.Repeat("x", 2048)))
				if err != nil {
					t.Fatal(err)
				}
				if format == handlers.LegacyBase64 {
					payload = []byte(base64.StdEncoding.EncodeToString(payload))
				}
				if err := conn.WriteMessage(libwebsocket.TextMessage, payload); err != nil {
					t.Fatal(err)
				}
				expectMessageTooBig(t, conn)
				select {
				case <-s.processed:
					t.Fatal("transport oversized event reached handler")
				default:
				}
			})
		}
	}
}

func TestHandlerReadLimitRemainsEffective(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(fmt.Sprintf("unknown=%t", unknown), func(t *testing.T) {
			upgrader := websocketstream.NewUpgrader([]string{testOrigin}, nil, websocketstream.Config{ReadLimit: 4096})
			if unknown {
				upgrader = unknownReadLimitUpgrader{upgrader}
			}
			s := newHardeningServerWithUpgrader(t, nil, upgrader,
				handlers.WithWireFormat(handlers.JSON), handlers.WithMessageLimits(1024, 1024),
			)
			conn := s.dial(t)
			if err := conn.WriteJSON(testevent.New(strings.Repeat("x", 2048))); err != nil {
				t.Fatal(err)
			}
			expectMessageTooBig(t, conn)
		})
	}
}

func TestTransportReadLimitCountsCompressedPayload(t *testing.T) {
	upgrader := websocketstream.NewUpgrader([]string{testOrigin}, nil, websocketstream.Config{
		ReadLimit: 1024, EnableCompression: true,
	})
	s := newHardeningServerWithUpgrader(t, nil, upgrader, handlers.WithWireFormat(handlers.JSON))
	conn := s.dial(t)
	conn.EnableWriteCompression(true)
	// The encoded frame is under 1 KiB, while the decoded message is above it.
	if err := conn.WriteJSON(testevent.New(strings.Repeat("x", 2048))); err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.processed:
	case <-time.After(2 * time.Second):
		t.Fatal("compressed event within transport and handler limits was not processed")
	}
}

func TestIdentityRejectedBeforeUpgrade(t *testing.T) {
	forTransports(t, func(t *testing.T, newServer serverFactory) {
		t.Helper()
		s := newServer(t, handlers.WithUserIDExtractor(func(fiber.Ctx) (eventstream.UserID, error) {
			return eventstream.UserIDNil, errors.New("invalid session")
		}), handlers.WithNetHTTPUserIDExtractor(func(*http.Request) (eventstream.UserID, error) {
			return eventstream.UserIDNil, errors.New("invalid session")
		}))
		conn, response, err := s.dialer.Dial("ws://localhost/ws", http.Header{originHeader: []string{testOrigin}})
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
	})
}

func TestOriginStatusPreserved(t *testing.T) {
	forTransports(t, func(t *testing.T, newServer serverFactory) {
		t.Helper()
		s := newServer(t)
		conn, response, err := s.dialer.Dial("ws://localhost/ws", http.Header{originHeader: []string{"https://not-allowed.example"}})
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
	})
}

type failingWriter struct{}

func (failingWriter) Write(any, io.Writer) error { return errors.New("intentional writer failure") }

func TestWriterFailureUnblocksReader(t *testing.T) {
	forTransports(t, func(t *testing.T, newServer serverFactory) {
		t.Helper()
		s := newServer(t, handlers.WithEventWriter(failingWriter{}))
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
	})
}

func TestNormalCloseAndConnectionLimit(t *testing.T) {
	forTransports(t, func(t *testing.T, newServer serverFactory) {
		t.Helper()
		s := newServer(t, handlers.WithMaxConnections(1))
		conn := s.dial(t)
		extra, response, err := s.dialer.Dial("ws://localhost/ws", http.Header{originHeader: []string{testOrigin}})
		if extra != nil {
			_ = extra.Close()
		}
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		if err == nil || response == nil || response.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("connection limit not enforced: %v %v", response, err)
		}
		closePayload := libwebsocket.FormatCloseMessage(libwebsocket.CloseNormalClosure, "")
		if err := conn.WriteControl(libwebsocket.CloseMessage, closePayload, time.Now().Add(time.Second)); err != nil {
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
	})
}

func TestZeroPingRejected(t *testing.T) {
	bus := inmem.New()
	defer bus.Close()
	_, err := handlers.NewHTTPHandler(handlers.NewOptions(slog.Default(), bus,
		websocketstream.NewUpgrader([]string{testOrigin}, nil), nil, "uid", handlers.WithPingPeriod(0)))
	if err == nil {
		t.Fatal("zero ping accepted")
	}
}

func TestShutdownChannelWithoutRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bus := inmem.New()
		defer bus.Close()
		shutdown := make(chan struct{})
		h, err := handlers.NewHTTPHandler(handlers.NewOptions(slog.Default(), bus,
			websocketstream.NewUpgrader([]string{testOrigin}, nil), shutdown, "uid"))
		if err != nil {
			t.Fatal(err)
		}
		defer h.Close()
		close(shutdown)
		synctest.Wait()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := h.Shutdown(ctx); err != nil {
			t.Fatalf("shutdown channel did not finish the unused handler: %v", err)
		}
	})
}

func TestClosedShutdownChannelRejectsFirstRequest(t *testing.T) {
	shutdown := make(chan struct{})
	close(shutdown)
	s := newHardeningServerWithShutdown(t, shutdown)
	conn, response, err := s.dialer.Dial("ws://localhost/ws", http.Header{originHeader: []string{testOrigin}})
	if conn != nil {
		_ = conn.Close()
	}
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("closed handler admitted its first request: %v %v", response, err)
	}
	if stats := s.h.Stats(); stats.Accepted != 0 || stats.Active != 0 || s.bus.Stats().Subscribers != 0 {
		t.Fatal("shutdown admitted work", stats)
	}
}

func TestMalformedAndUnsupportedMessages(t *testing.T) {
	forTransports(t, func(t *testing.T, newServer serverFactory) {
		t.Helper()
		cases := []struct {
			name    string
			kind    int
			payload []byte
			code    int
		}{
			{name: "binary", kind: libwebsocket.BinaryMessage, payload: []byte(`{}`), code: libwebsocket.CloseUnsupportedData},
			{name: "JSON", kind: libwebsocket.TextMessage, payload: []byte(`{"eventType":`), code: libwebsocket.ClosePolicyViolation},
			{name: "UTF8", kind: libwebsocket.TextMessage, payload: []byte{255}, code: libwebsocket.ClosePolicyViolation},
			{
				name: "unknown", kind: libwebsocket.TextMessage, payload: []byte(`{"eventType":"unknown"}`),
				code: libwebsocket.ClosePolicyViolation,
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				s := newServer(t, handlers.WithWireFormat(handlers.JSON))
				conn := s.dial(t)
				if err := conn.WriteMessage(tc.kind, tc.payload); err != nil {
					t.Fatal(err)
				}
				_, _, err := conn.ReadMessage()
				if !libwebsocket.IsCloseError(err, tc.code) {
					t.Fatalf("unexpected close code: %v", err)
				}
				if s.h.Stats().Incoming != 0 {
					t.Fatal("rejected message was admitted")
				}
			})
		}
	})
}

type overloadedProcessor struct{}

func (overloadedProcessor) Process(context.Context, eventstream.Event) { panic("Submit must be used") }

func (overloadedProcessor) Submit(context.Context, eventstream.Event) error {
	return eventprocessor.ErrOverloaded
}

func TestProcessorOverloadClosesWithRetryCode(t *testing.T) {
	forTransports(t, func(t *testing.T, newServer serverFactory) {
		t.Helper()
		s := newServer(t, handlers.WithWireFormat(handlers.JSON), handlers.WithReadEventProcessor(overloadedProcessor{}))
		conn := s.dial(t)
		if err := conn.WriteJSON(testevent.New("overload")); err != nil {
			t.Fatal(err)
		}
		_, _, err := conn.ReadMessage()
		if !libwebsocket.IsCloseError(err, libwebsocket.CloseTryAgainLater) {
			t.Fatalf("overload did not close with 1013: %v", err)
		}
		if s.h.Stats().Incoming != 0 {
			t.Fatal("overloaded message was admitted")
		}
	})
}

type failingSubmitter struct{ err error }

func (failingSubmitter) Process(context.Context, eventstream.Event) { panic("Submit must be used") }

func (p failingSubmitter) Submit(context.Context, eventstream.Event) error { return p.err }

func TestProcessorCancellationIsNotPolicyViolation(t *testing.T) {
	forTransports(t, func(t *testing.T, newServer serverFactory) {
		t.Helper()
		cases := []struct {
			name string
			err  error
			code int
		}{
			{name: "canceled", err: context.Canceled, code: libwebsocket.CloseNormalClosure},
			{name: "wrapped canceled", err: fmt.Errorf("submit: %w", context.Canceled), code: libwebsocket.CloseNormalClosure},
			{name: "deadline", err: context.DeadlineExceeded, code: libwebsocket.CloseInternalServerErr},
			{
				name: "wrapped deadline", err: fmt.Errorf("submit: %w", context.DeadlineExceeded),
				code: libwebsocket.CloseInternalServerErr,
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				s := newServer(t, handlers.WithWireFormat(handlers.JSON),
					handlers.WithReadEventProcessor(failingSubmitter{err: tc.err}))
				conn := s.dial(t)
				// Keep the connection context live so the pump result determines the close.
				if err := conn.WriteJSON(testevent.New("submission canceled")); err != nil {
					t.Fatal(err)
				}
				_, _, err := conn.ReadMessage()
				if !libwebsocket.IsCloseError(err, tc.code) {
					t.Fatalf("submission cancellation closed with the wrong code: %v", err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				if err := s.h.Shutdown(ctx); err != nil {
					t.Fatal(err)
				}
				stats := s.h.Stats()
				if stats.PolicyClosed != 0 || stats.Incoming != 0 {
					t.Fatal("canceled submission counted as a policy violation or accepted message", stats)
				}
				if tc.code == libwebsocket.CloseNormalClosure && stats.NormalClosed != 1 {
					t.Fatal("cancellation was not counted as a normal close", stats)
				}
				if tc.code == libwebsocket.CloseInternalServerErr && stats.InternalClosed != 1 {
					t.Fatal("deadline was not counted as an internal close", stats)
				}
			})
		}
	})
}

func TestOutboundLimitRejectsWithoutDeliveringPayload(t *testing.T) {
	forTransports(t, func(t *testing.T, newServer serverFactory) {
		t.Helper()
		s := newServer(t, handlers.WithMaxOutboundBytes(64))
		conn := s.dial(t)
		select {
		case <-s.bus.ready:
		case <-time.After(time.Second):
			t.Fatal("no subscription")
		}
		if err := s.bus.Publish(context.Background(), s.uid, testevent.New("outgoing")); err != nil {
			t.Fatal(err)
		}
		_, data, err := conn.ReadMessage()
		if !libwebsocket.IsCloseError(err, libwebsocket.CloseMessageTooBig) || len(data) != 0 {
			t.Fatalf("oversized output was delivered: %q %v", data, err)
		}
		if s.h.Stats().Outgoing != 0 {
			t.Fatal("oversized output counted as delivered")
		}
	})
}

func TestSubscriptionFailureSendsInternalClose(t *testing.T) {
	forTransports(t, func(t *testing.T, newServer serverFactory) {
		t.Helper()
		cases := []struct {
			name      string
			subscribe func(context.Context, eventstream.UserID) (<-chan eventstream.Event, error)
		}{
			{name: "nil channel", subscribe: func(context.Context, eventstream.UserID) (<-chan eventstream.Event, error) {
				//nolint:nilnil // Simulate a stream violating the Subscribe contract.
				return nil, nil
			}},
			{name: failurePanic, subscribe: func(context.Context, eventstream.UserID) (<-chan eventstream.Event, error) {
				panic("subscription failed")
			}},
			{name: failureError, subscribe: func(context.Context, eventstream.UserID) (<-chan eventstream.Event, error) {
				return nil, errors.New("subscription error")
			}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				s := newServer(t)
				s.bus.subscribe = tc.subscribe
				conn := s.dial(t)
				_, _, err := conn.ReadMessage()
				if !libwebsocket.IsCloseError(err, libwebsocket.CloseInternalServerErr) {
					t.Fatalf("subscription failure did not close with 1011: %v", err)
				}
				if s.h.Stats().InternalClosed != 1 {
					t.Fatal("subscription close was not counted")
				}
			})
		}
	})
}

type pendingUpgrader struct{ websocketstream.Upgrader }

func (pendingUpgrader) UpgradeFastHTTP(*fasthttp.RequestCtx, libwebsocket.FastHTTPHandler) error {
	return nil
}

func TestPendingUpgradeReleasesAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bus := inmem.New()
		defer bus.Close()
		u := pendingUpgrader{websocketstream.NewUpgrader([]string{testOrigin}, nil)}
		h, err := handlers.NewHTTPHandler(handlers.NewOptions(slog.Default(), bus, u, nil, "uid",
			handlers.WithMaxConnections(1), handlers.WithHandoffTimeout(time.Second)))
		if err != nil {
			t.Fatal(err)
		}
		defer h.Close()
		app := fiber.New()
		ctx := app.AcquireCtx(&fasthttp.RequestCtx{})
		defer app.ReleaseCtx(ctx)
		ctx.Locals("uid", eventstream.NewUserID())
		if err := h.Serve(ctx); err != nil {
			t.Fatal(err)
		}
		if h.Stats().Active != 1 {
			t.Fatal("pending upgrade did not reserve admission")
		}
		// Advance the handoff deadline using the bubble's virtual clock.
		time.Sleep(time.Second)
		synctest.Wait()
		if stats := h.Stats(); stats.Active != 0 || stats.Accepted != 0 || bus.Stats().Subscribers != 0 {
			t.Fatal("expired handoff retained work", stats)
		}
		if err := h.Serve(ctx); err != nil {
			t.Fatal("expired handoff did not free admission", err)
		}
		if err := h.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		if h.Stats().Active != 0 {
			t.Fatal("shutdown retained a pending upgrade")
		}
	})
}
