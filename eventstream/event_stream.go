package eventstream

import (
	"context"
	"io"
)

//go:generate toolsmocks

type Event interface {
	EventID() EventID
	EventName() string
	Validate() error
}

type EventStream interface {
	io.Closer
	Subscribe(ctx context.Context, userID UserID) (<-chan Event, error)
	Publish(ctx context.Context, userID UserID, event Event) error
}
