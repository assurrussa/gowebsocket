// Package eventadapter validates and converts application events and JSON.
package eventadapter

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"

	"github.com/assurrussa/gowebsocket/eventstream"
	"github.com/assurrussa/gowebsocket/internal/safety"
)

type EventAdapter interface {
	Adapt(eventstream.Event) (any, error)
	ReverseAdapt([]byte) (eventstream.Event, error)
}

var ErrUnknownEventType = errors.New("unknown event type")

type Options struct{ processors map[string]EventAdapter }
type OptOptionsSetter func(*Options)

func NewOptions(options ...OptOptionsSetter) Options {
	var o Options
	for _, option := range options {
		if option != nil {
			option(&o)
		}
	}
	return o
}
func WithProcessors(value map[string]EventAdapter) OptOptionsSetter {
	return func(o *Options) { o.processors = maps.Clone(value) }
}
func (o *Options) Validate() error {
	for name, adapter := range o.processors {
		if name == "" || safety.IsNil(adapter) {
			return errors.New("invalid event adapter registration")
		}
	}
	return nil
}

type Adapter struct{ Options }

func NewAdapter(opts Options) (*Adapter, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	opts.processors = maps.Clone(opts.processors)
	return &Adapter{Options: opts}, nil
}

func (a *Adapter) Adapt(event eventstream.Event) (result any, err error) {
	if err := eventstream.ValidateEvent(event); err != nil {
		return nil, err
	}
	err = safety.Call(func() error {
		processor, ok := a.processors[event.EventName()]
		if !ok {
			return ErrUnknownEventType
		}
		var err error
		result, err = processor.Adapt(event)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (a *Adapter) ReverseAdapt(message []byte) (event eventstream.Event, err error) {
	var meta struct {
		EventName string `json:"eventType"`
	}
	if err := json.Unmarshal(message, &meta); err != nil {
		return nil, errors.New("invalid event JSON")
	}
	processor, ok := a.processors[meta.EventName]
	if !ok {
		return nil, ErrUnknownEventType
	} // Never include the message body.
	err = safety.Call(func() error {
		var err error
		event, err = processor.ReverseAdapt(message)
		if err != nil {
			return errors.New("invalid event payload")
		}
		if err := eventstream.ValidateEvent(event); err != nil {
			return err
		}
		if event.EventName() != meta.EventName {
			return errors.New("event type mismatch")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return event, nil
}

type EventProcessor[T eventstream.Event] struct{}

func NewEventProcessor[T eventstream.Event]() *EventProcessor[T] { return &EventProcessor[T]{} }
func (*EventProcessor[T]) Adapt(event eventstream.Event) (result any, err error) {
	if err := eventstream.ValidateEvent(event); err != nil {
		return nil, err
	}
	if _, ok := event.(T); !ok {
		return nil, errors.New("event Go type mismatch")
	}
	err = safety.Call(func() error { var err error; result, err = json.Marshal(event); return err })
	return result, err
}
func (*EventProcessor[T]) ReverseAdapt(message []byte) (result eventstream.Event, err error) {
	err = safety.Call(func() error {
		var event T
		if err := json.Unmarshal(message, &event); err != nil {
			return errors.New("invalid event payload")
		}
		if err := eventstream.ValidateEvent(event); err != nil {
			return err
		}
		result = event
		return nil
	})
	return result, err
}

// Envelope is the opt-in v1 wire format. Register EnvelopeEventProcessor for
// both directions. The legacy flat format remains unchanged by default.
type Envelope struct {
	Version   int                 `json:"version"`
	EventID   eventstream.EventID `json:"eventId"`
	EventType string              `json:"eventType"`
	Payload   json.RawMessage     `json:"payload"`
}

type EnvelopeEventProcessor[T eventstream.Event] struct{}

func NewEnvelopeEventProcessor[T eventstream.Event]() *EnvelopeEventProcessor[T] {
	return &EnvelopeEventProcessor[T]{}
}
func (*EnvelopeEventProcessor[T]) Adapt(event eventstream.Event) (result any, err error) {
	err = safety.Call(func() error {
		raw, err := NewEventProcessor[T]().Adapt(event)
		if err != nil {
			return err
		}
		data, ok := raw.([]byte)
		if !ok {
			return errors.New("invalid encoded event")
		}
		result = Envelope{Version: 1, EventID: event.EventID(), EventType: event.EventName(), Payload: data}
		return nil
	})
	return result, err
}
func (*EnvelopeEventProcessor[T]) ReverseAdapt(message []byte) (result eventstream.Event, err error) {
	err = safety.Call(func() error {
		var envelope Envelope
		if err := json.Unmarshal(message, &envelope); err != nil {
			return errors.New("invalid envelope")
		}
		if envelope.Version != 1 {
			return errors.New("unsupported envelope version")
		}
		event, err := NewEventProcessor[T]().ReverseAdapt(envelope.Payload)
		if err != nil {
			return err
		}
		if event.EventName() != envelope.EventType || event.EventID() != envelope.EventID {
			return fmt.Errorf("envelope metadata mismatch: %w", eventstream.ErrInvalidEvent)
		}
		result = event
		return nil
	})
	return result, err
}
