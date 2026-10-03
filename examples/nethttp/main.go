// Command nethttp runs a localhost-only net/http WebSocket echo example.
package main

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	_ "embed"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/assurrussa/gowebsocket/eventstream"
	inmem "github.com/assurrussa/gowebsocket/eventstream/inmem"
	"github.com/assurrussa/gowebsocket/websocketstream"
	"github.com/assurrussa/gowebsocket/websocketstream/eventadapter"
	"github.com/assurrussa/gowebsocket/websocketstream/eventprocessor"
	"github.com/assurrussa/gowebsocket/websocketstream/handlers"
)

//go:embed index.html
var page string

const echoEventType = "echo"

type echoEvent struct {
	ID   eventstream.EventID `json:"eventId"`
	Type string              `json:"eventType"`
	Body string              `json:"body"`
}

func (e *echoEvent) EventID() eventstream.EventID { return e.ID }
func (e *echoEvent) EventName() string            { return e.Type }
func (e *echoEvent) Validate() error {
	if e == nil || e.ID.IsZero() || e.Type != echoEventType || e.Body == "" || len(e.Body) > 4000 {
		return errors.New("invalid echo event")
	}
	return nil
}

type echoProcessor struct{ bus *inmem.Service }

func (p echoProcessor) Handle(ctx context.Context, event eventstream.Event) error {
	uid, ok := eventstream.UserIDFromContext(ctx)
	if !ok {
		return errors.New("no authenticated identity")
	}

	typed, ok := event.(*echoEvent)
	if !ok {
		return errors.New("unexpected event type")
	}
	return p.bus.Publish(ctx, uid, typed)
}

func run() error {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return err
	}
	token := base64.RawURLEncoding.EncodeToString(secret)
	uid := eventstream.NewUserID()
	bus := inmem.New()
	defer bus.Close()
	upgrader, err := websocketstream.NewUpgraderChecked([]string{"http://127.0.0.1:8081"}, nil, websocketstream.Config{})
	if err != nil {
		return err
	}
	h, err := handlers.NewHTTPHandler(handlers.NewOptions(slog.Default(), bus, upgrader, nil, "",
		handlers.WithNetHTTPUserIDExtractor(func(r *http.Request) (eventstream.UserID, error) {
			cookie, cookieErr := r.Cookie("demo_session")
			if cookieErr != nil || subtle.ConstantTimeCompare([]byte(cookie.Value), []byte(token)) != 1 {
				return eventstream.UserIDNil, errors.New("invalid demo session")
			}
			return uid, nil
		}),
		handlers.WithEventAdapters(map[string]eventadapter.EventAdapter{echoEventType: eventadapter.NewEventProcessor[*echoEvent]()}),
		handlers.WithEventProcessors(map[string]eventprocessor.EventProcessor{echoEventType: echoProcessor{bus: bus}}),
	))
	if err != nil {
		return err
	}
	defer h.Close()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		// Demo session only, not an authentication system. Bind to loopback and
		// replace this with real application authentication before deployment.
		//nolint:gosec // Loopback-only HTTP demo; production sessions require HTTPS and Secure.
		http.SetCookie(w, &http.Cookie{
			Name: "demo_session", Value: token, HttpOnly: true,
			SameSite: http.SameSiteStrictMode, Path: "/",
		})
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		if _, writeErr := io.WriteString(w, page); writeErr != nil {
			slog.Error("write example page", "error", writeErr)
		}
	})
	mux.Handle("/ws", h)
	server := &http.Server{
		Addr:              "127.0.0.1:8081",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serverErr := make(chan error, 1)
	go func() { serverErr <- server.ListenAndServe() }()

	slog.Info("open http://127.0.0.1:8081 (localhost demo only)")
	select {
	case <-ctx.Done():
	case err = <-serverErr:
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Server.Shutdown does not close hijacked WebSocket connections. Seal
	// WebSocket admission and finish the handler before closing its event stream.
	return errors.Join(err, h.Shutdown(shutdown), bus.Shutdown(shutdown), server.Shutdown(shutdown))
}

func main() {
	if err := run(); err != nil {
		slog.Error("example stopped", "error", err)
	}
}
