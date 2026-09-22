package eventstream

import (
	"context"
	"io"

	"github.com/assurrussa/goshared/pkg/sharedtypes"
)

//go:generate toolsmocks

type Event interface {
	EventID() sharedtypes.EventID
	EventName() string
	Validate() error
}

type EventStream interface {
	io.Closer
	Subscribe(ctx context.Context, userID sharedtypes.UserID) (<-chan Event, error)
	Publish(ctx context.Context, userID sharedtypes.UserID, event Event) error
}
