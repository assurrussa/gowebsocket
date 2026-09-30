// Package websocketstream defines realtime transport contracts.
package websocketstream

import (
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/assurrussa/gowebsocket/eventstream"
)

type EventAdapter interface {
	Adapt(event eventstream.Event) (any, error)
	ReverseAdapt(payload []byte) (eventstream.Event, error)
}

type ReadEventProcessor interface {
	Process(ctx context.Context, event eventstream.Event)
}

// EventSubmitter allows HTTPHandler to observe overload without blocking reads.
type EventSubmitter interface {
	Submit(ctx context.Context, event eventstream.Event) error
}

type EventWriter interface {
	Write(event any, out io.Writer) error
}

// JSONEventWriter preserves the legacy raw []byte/string API, but validates the
// JSON before writing. Prefer RawMessage or structured values in new code.
type JSONEventWriter struct{}

func (JSONEventWriter) Write(event any, out io.Writer) error {
	var raw []byte
	switch data := event.(type) {
	case json.RawMessage:
		raw = data
	case []byte:
		raw = data
	case string:
		raw = []byte(data)
	default:
		return json.NewEncoder(out).Encode(event)
	}
	if !json.Valid(raw) {
		return errors.New("invalid raw JSON event")
	}
	n, err := out.Write(raw)
	if err == nil && n != len(raw) {
		return io.ErrShortWrite
	}
	return err
}

// StrictJSONEventWriter uses standard encoding/json semantics: strings become
// JSON strings and []byte becomes base64. RawMessage is the explicit raw escape.
type StrictJSONEventWriter struct{}

func (StrictJSONEventWriter) Write(event any, out io.Writer) error {
	return json.NewEncoder(out).Encode(event)
}
