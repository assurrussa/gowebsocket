package handlers_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	libwebsocket "github.com/fasthttp/websocket"

	"github.com/assurrussa/gowebsocket/eventstream"
	inmem "github.com/assurrussa/gowebsocket/eventstream/inmem"
	"github.com/assurrussa/gowebsocket/internal/testevent"
	"github.com/assurrussa/gowebsocket/websocketstream"
	"github.com/assurrussa/gowebsocket/websocketstream/eventadapter"
	"github.com/assurrussa/gowebsocket/websocketstream/eventprocessor"
	"github.com/assurrussa/gowebsocket/websocketstream/handlers"
)

const (
	testEventType  = "test"
	failureError   = "error"
	failurePanic   = "panic"
	failureHijack  = "hijack-panic"
	failureWritten = "written"
)

type serverFactory func(*testing.T, ...handlers.OptOptionsSetter) *testServer

func forTransports(t *testing.T, test func(*testing.T, serverFactory)) {
	t.Helper()
	for _, transport := range []struct {
		name   string
		create serverFactory
	}{
		{name: "fiber", create: newHardeningServer},
		{name: "nethttp", create: newNetHTTPServer},
	} {
		t.Run(transport.name, func(t *testing.T) { test(t, transport.create) })
	}
}

func newNetHTTPServer(t *testing.T, options ...handlers.OptOptionsSetter) *testServer {
	t.Helper()
	return newNetHTTPServerWithUpgrader(t, websocketstream.NewUpgrader([]string{testOrigin}, nil,
		websocketstream.Config{EnableCompression: true}), options...)
}

func newNetHTTPServerWithUpgrader(t *testing.T, upgrader websocketstream.Upgrader,
	options ...handlers.OptOptionsSetter,
) *testServer {
	t.Helper()
	s := &testServer{
		uid: eventstream.NewUserID(), processed: make(chan checkedEvent, 16),
		bus: &readyStream{Service: inmem.New(), ready: make(chan struct{})},
	}
	defaults := make([]handlers.OptOptionsSetter, 0, 3+len(options))
	defaults = append(defaults,
		handlers.WithNetHTTPUserIDExtractor(func(*http.Request) (eventstream.UserID, error) { return s.uid, nil }),
		handlers.WithEventAdapters(map[string]eventadapter.EventAdapter{
			testEventType: eventadapter.NewEventProcessor[*testevent.Event](),
		}),
		handlers.WithEventProcessors(map[string]eventprocessor.EventProcessor{testEventType: checkedProcessor{output: s.processed}}),
	)
	defaults = append(defaults, options...)
	var err error
	s.h, err = handlers.NewHTTPHandler(handlers.NewOptions(slog.New(slog.DiscardHandler), s.bus, upgrader, nil, "", defaults...))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(s.h)
	s.dialer = &libwebsocket.Dialer{
		NetDialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", server.Listener.Addr().String())
		},
		HandshakeTimeout: 2 * time.Second, EnableCompression: true,
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := s.h.Shutdown(ctx); err != nil {
			t.Error(err)
		}
		if err := s.bus.Shutdown(ctx); err != nil {
			t.Error(err)
		}
		server.Close()
	})
	return s
}

func TestNetHTTPExtractorFailsClosed(t *testing.T) {
	for _, kind := range []string{"missing", "zero", failureError, failurePanic} {
		t.Run(kind, func(t *testing.T) {
			bus := inmem.New()
			defer bus.Close()
			upgrader := &countingHTTPUpgrader{Upgrader: websocketstream.NewUpgrader([]string{testOrigin}, nil)}
			var extract handlers.NetHTTPUserIDExtractor
			if kind != "missing" {
				extract = func(*http.Request) (eventstream.UserID, error) {
					switch kind {
					case failureError:
						return eventstream.UserIDNil, errors.New("private auth failure")
					case failurePanic:
						panic("private auth panic")
					default:
						return eventstream.UserIDNil, nil
					}
				}
			}
			// A legacy Fiber key does not authorize net/http requests.
			h, err := handlers.NewHTTPHandler(handlers.NewOptions(slog.Default(), bus, upgrader, nil, "uid",
				handlers.WithNetHTTPUserIDExtractor(extract)))
			if err != nil {
				t.Fatal(err)
			}
			defer h.Close()
			response := httptest.NewRecorder()
			h.ServeHTTP(response, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/ws", nil))
			if response.Code != http.StatusUnauthorized || upgrader.calls.Load() != 0 {
				t.Fatalf("authentication did not reject before upgrade: %d, calls %d", response.Code, upgrader.calls.Load())
			}
			if response.Body.String() != "Unauthorized\n" {
				t.Fatal("authentication detail exposed")
			}
			if stats := h.Stats(); stats.Active != 0 || stats.Accepted != 0 || stats.Rejected != 1 || bus.Stats().Subscribers != 0 {
				t.Fatal("rejected auth retained work", stats)
			}
		})
	}
}

type countingHTTPUpgrader struct {
	websocketstream.Upgrader
	calls   atomic.Int64
	upgrade func(http.ResponseWriter, *http.Request) (websocketstream.Websocket, error)
}

func (u *countingHTTPUpgrader) Upgrade(w http.ResponseWriter, r *http.Request, headers http.Header,
) (websocketstream.Websocket, error) {
	u.calls.Add(1)
	if u.upgrade != nil {
		return u.upgrade(w, r)
	}
	return u.Upgrader.Upgrade(w, r, headers)
}

func TestNetHTTPUpgradeFailuresReleaseAdmission(t *testing.T) {
	for _, kind := range []string{failureError, failurePanic, "nil", failureWritten, failureHijack} {
		t.Run(kind, func(t *testing.T) {
			upgrader := &countingHTTPUpgrader{upgrade: failedUpgrade(kind)}
			s := newNetHTTPServerWithUpgrader(t, upgrader, handlers.WithMaxConnections(1))
			conn, response, err := s.dialer.Dial("ws://localhost/ws", http.Header{originHeader: []string{testOrigin}})
			if conn != nil {
				_ = conn.Close()
			}
			if response != nil {
				_ = response.Body.Close()
			}
			if err == nil {
				t.Fatal("broken upgrade succeeded")
			}
			want := http.StatusInternalServerError
			if kind == failureWritten {
				want = http.StatusForbidden
			}
			if kind != failureHijack && (response == nil || response.StatusCode != want) {
				t.Fatalf("status not preserved: %v", response)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := s.h.Shutdown(ctx); err != nil {
				t.Fatal(err)
			}
			if stats := s.h.Stats(); stats.Active != 0 || stats.Accepted != 0 || stats.Rejected != 1 {
				t.Fatal("upgrade leaked admission", stats)
			}
		})
	}
}

func failedUpgrade(kind string) func(http.ResponseWriter, *http.Request) (websocketstream.Websocket, error) {
	return func(w http.ResponseWriter, _ *http.Request) (websocketstream.Websocket, error) {
		switch kind {
		case failurePanic:
			panic("private upgrade panic")
		case "nil":
			return nil, nil
		case failureWritten:
			http.Error(w, "denied", http.StatusForbidden)
		case failureHijack:
			conn, _, err := http.NewResponseController(w).Hijack()
			if err != nil {
				return nil, err
			}
			_ = conn.SetDeadline(time.Now().Add(time.Second))
			panic("after hijack")
		}
		return nil, errors.New("private upgrade failure")
	}
}

func TestTransportPongTimeoutAndSubscriptionEnd(t *testing.T) {
	forTransports(t, func(t *testing.T, newServer serverFactory) {
		t.Helper()
		t.Run("pong deadline", func(t *testing.T) {
			s := newServer(t, handlers.WithPingPeriod(100*time.Millisecond), handlers.WithPongWait(250*time.Millisecond))
			conn := s.dial(t)
			conn.SetPingHandler(func(string) error { return nil })
			if _, _, err := conn.ReadMessage(); !libwebsocket.IsCloseError(err, libwebsocket.CloseInternalServerErr) {
				t.Fatalf("missing pong did not terminate connection: %v", err)
			}
		})
		t.Run("subscription terminated", func(t *testing.T) {
			s := newServer(t)
			s.bus.subscribe = func(context.Context, eventstream.UserID) (<-chan eventstream.Event, error) {
				events := make(chan eventstream.Event)
				close(events)
				return events, nil
			}
			conn := s.dial(t)
			if _, _, err := conn.ReadMessage(); !libwebsocket.IsCloseError(err, libwebsocket.CloseTryAgainLater) {
				t.Fatalf("terminated delivery did not request resync: %v", err)
			}
		})
	})
}

func TestNetHTTPShutdownOverlapsAuthentication(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	bus := inmem.New()
	defer bus.Close()
	upgrader := &countingHTTPUpgrader{Upgrader: websocketstream.NewUpgrader([]string{testOrigin}, nil)}
	h, err := handlers.NewHTTPHandler(handlers.NewOptions(slog.Default(), bus, upgrader, nil, "",
		handlers.WithNetHTTPUserIDExtractor(func(*http.Request) (eventstream.UserID, error) {
			close(entered)
			<-release
			return eventstream.NewUserID(), nil
		})))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(response, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/ws", nil))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("auth did not start")
	}
	if err := h.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	close(release)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("request did not end")
	}
	if response.Code != http.StatusServiceUnavailable || upgrader.calls.Load() != 0 || h.Stats().Active != 0 {
		t.Fatal("shutdown during auth admitted work", response.Code, h.Stats())
	}
}

type closedSocket struct {
	websocketstream.Websocket
	closed chan struct{}
}

func (s *closedSocket) Close() error { close(s.closed); return nil }

func TestNetHTTPLateUpgradeClosesWithoutPumps(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		socket := &closedSocket{closed: make(chan struct{})}
		upgrader := &countingHTTPUpgrader{upgrade: func(http.ResponseWriter, *http.Request) (websocketstream.Websocket, error) {
			close(entered)
			<-release
			return socket, nil
		}}
		bus := inmem.New()
		defer bus.Close()
		h, err := handlers.NewHTTPHandler(handlers.NewOptions(slog.Default(), bus, upgrader, nil, "",
			handlers.WithHandoffTimeout(time.Second),
			handlers.WithNetHTTPUserIDExtractor(func(*http.Request) (eventstream.UserID, error) { return eventstream.NewUserID(), nil })))
		if err != nil {
			t.Fatal(err)
		}
		defer h.Close()
		done := make(chan struct{})
		go func() {
			defer close(done)
			h.ServeHTTP(httptest.NewRecorder(),
				httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/ws", nil))
		}()
		<-entered
		if h.Stats().Active != 1 {
			t.Fatal("no pending admission")
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if h.Stats().Active != 0 {
			t.Fatal("handoff timeout leaked admission")
		}
		if err := h.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		close(release)
		<-done
		select {
		case <-socket.closed:
		default:
			t.Fatal("late socket was not closed")
		}
		if h.Stats().Accepted != 0 || bus.Stats().Subscribers != 0 {
			t.Fatal("late socket started pumps")
		}
	})
}

func TestNetHTTPInformationalAndFlushedResponses(t *testing.T) {
	for _, flush := range []bool{false, true} {
		name := "early hints"
		if flush {
			name = "flushed"
		}
		t.Run(name, func(t *testing.T) {
			upgrader := &countingHTTPUpgrader{upgrade: func(w http.ResponseWriter, _ *http.Request) (websocketstream.Websocket, error) {
				w.WriteHeader(http.StatusEarlyHints)
				if flush {
					if err := http.NewResponseController(w).Flush(); err != nil {
						return nil, err
					}
				}
				return nil, errors.New("upgrade failed after interim response")
			}}
			s := newNetHTTPServerWithUpgrader(t, upgrader)
			transport := &http.Transport{DialContext: s.dialer.NetDialContext}
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport, Timeout: time.Second}
			request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "http://localhost/ws", nil)
			request.RequestURI = ""
			response, err := client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			want, wantBody := http.StatusInternalServerError, "websocket upgrade failed\n"
			if flush {
				want, wantBody = http.StatusOK, ""
			}
			if response.StatusCode != want || string(body) != wantBody {
				t.Fatalf("response after failed upgrade: status %d body %q", response.StatusCode, body)
			}
		})
	}
}
