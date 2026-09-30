// Command basic runs a localhost-only WebSocket echo example.
package main

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	_ "embed"
	"encoding/base64"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v3"

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
	upgrader, err := websocketstream.NewUpgraderChecked([]string{"http://127.0.0.1:8080"}, nil, websocketstream.Config{})
	if err != nil {
		return err
	}
	h, err := handlers.NewHTTPHandler(handlers.NewOptions(slog.Default(), bus, upgrader, nil, "",
		handlers.WithUserIDExtractor(func(c fiber.Ctx) (eventstream.UserID, error) {
			if subtle.ConstantTimeCompare([]byte(c.Cookies("demo_session")), []byte(token)) != 1 {
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
	app := fiber.New(fiber.Config{
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	})
	app.Get("/", func(c fiber.Ctx) error {
		// Demo session only, not an authentication system. Bind to loopback and
		// replace this with real application authentication before deployment.
		c.Cookie(&fiber.Cookie{Name: "demo_session", Value: token, HTTPOnly: true, SameSite: "Strict", Path: "/"})
		c.Type("html")
		return c.SendString(page)
	})
	app.Get("/ws", h.Serve)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serverErr := make(chan error, 1)
	go func() { serverErr <- app.Listen("127.0.0.1:8080", fiber.ListenConfig{DisableStartupMessage: true}) }()
	slog.Info("open http://127.0.0.1:8080 (localhost demo only)")
	select {
	case <-ctx.Done():
	case err = <-serverErr:
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return errors.Join(err, h.Shutdown(shutdown), bus.Shutdown(shutdown), app.Shutdown())
}

func main() {
	if err := run(); err != nil {
		slog.Error("example stopped", "error", err)
	}
}
