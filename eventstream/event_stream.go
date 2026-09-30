// Package eventstream defines local realtime events and trusted connection identity.
package eventstream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"

	"github.com/assurrussa/gowebsocket/internal/safety"
)

//go:generate go run go.uber.org/mock/mockgen@v0.6.0 -source=event_stream.go -destination=mocks/event_stream_mock.gen.go -package=eventstreammocks

// Event is an application event. EventName must be stable, nonempty and at most
// 128 UTF-8 bytes. Events sent through inmem must survive a JSON round trip.
type Event interface {
	EventID() EventID
	EventName() string
	Validate() error
}

// EventStream offers best-effort, process-local fanout, not durable delivery.
// Publish success does not acknowledge browser receipt. Offline events are not
// stored. Implementations document their overflow and payload ownership policy.
type EventStream interface {
	io.Closer
	Subscribe(ctx context.Context, userID UserID) (<-chan Event, error)
	Publish(ctx context.Context, userID UserID, event Event) error
}

var ErrInvalidEvent = errors.New("invalid event")

// ValidateEvent is the shared trust-boundary check, including typed nil and
// panicking application methods. Callers must not log an untrusted error body.
func ValidateEvent(event Event) error {
	if safety.IsNil(event) {
		return ErrInvalidEvent
	}
	err := safety.Call(func() error {
		name := event.EventName()
		if name == "" || len(name) > 128 || !utf8.ValidString(name) {
			return ErrInvalidEvent
		}
		return event.Validate()
	})
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidEvent, err)
	}
	return nil
}

type userIDKey struct{}

// WithUserID attaches an authenticated identity. Only server-side code should
// call it; never populate it from a client-supplied message field.
func WithUserID(ctx context.Context, id UserID) context.Context {
	return context.WithValue(ctx, userIDKey{}, id)
}

func UserIDFromContext(ctx context.Context) (UserID, bool) {
	id, ok := ctx.Value(userIDKey{}).(UserID)
	return id, ok && !id.IsZero()
}

// Logger is satisfied by *slog.Logger and gologger.Logger. The library does not
// own or close the logger and does not require a project-specific logging API.
type Logger interface {
	DebugContext(ctx context.Context, msg string, args ...any)
	WarnContext(ctx context.Context, msg string, args ...any)
	ErrorContext(ctx context.Context, msg string, args ...any)
}
