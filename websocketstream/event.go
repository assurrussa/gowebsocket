package websocketstream

import (
	"context"
	"io"

	"github.com/goccy/go-json"

	eventstream "github.com/assurrussa/gowebsocket/eventstream"
)

// EventAdapter converts the event from the stream to the appropriate object.
type EventAdapter interface {
	Adapt(event eventstream.Event) (any, error)
	ReverseAdapt(message []byte) (eventstream.Event, error)
}

// ReadEventProcessor process read event new message from client.
type ReadEventProcessor interface {
	Process(ctx context.Context, event eventstream.Event)
}

// EventWriter write adapted event it to the socket.
type EventWriter interface {
	Write(event any, out io.Writer) error
}

type JSONEventWriter struct{}

func (JSONEventWriter) Write(event any, out io.Writer) error {
	switch data := event.(type) {
	case []byte:
		if _, err := out.Write(data); err != nil {
			return err
		}

		return nil
	case string:
		if _, err := out.Write([]byte(data)); err != nil {
			return err
		}

		return nil
	}

	return json.NewEncoder(out).Encode(event)
}
