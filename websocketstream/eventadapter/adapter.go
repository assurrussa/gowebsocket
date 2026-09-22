package eventadapter

import (
	"encoding/json"
	"errors"
	"fmt"

	eventstream "github.com/assurrussa/gowebsocket/eventstream"
)

type EventAdapter interface {
	Adapt(event eventstream.Event) (any, error)
	ReverseAdapt(message []byte) (eventstream.Event, error)
}

var ErrUnknownEventType = errors.New("unknown event type")

//go:generate options-gen -out-filename=adapter_options.gen.go -from-struct=Options
type Options struct {
	processors map[string]EventAdapter
}

type Adapter struct {
	Options
}

func NewAdapter(opts Options) (*Adapter, error) {
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("validate options: %w", err)
	}

	return &Adapter{
		Options: opts,
	}, nil
}

func (a *Adapter) Adapt(event eventstream.Event) (any, error) {
	if processor, ok := a.processors[event.EventName()]; ok {
		res, err := processor.Adapt(event)
		if err != nil {
			return nil, fmt.Errorf("process event %s: %w", event.EventName(), err)
		}

		return res, nil
	}

	return nil, fmt.Errorf("unknown client event: %s (%T): %w", event.EventName(), event, ErrUnknownEventType)
}

func (a *Adapter) ReverseAdapt(message []byte) (eventstream.Event, error) {
	var meta struct {
		EventName string `json:"eventType"`
	}
	if err := json.Unmarshal(message, &meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal event metadata: %w", err)
	}

	if processor, ok := a.processors[meta.EventName]; ok {
		res, err := processor.ReverseAdapt(message)
		if err != nil {
			return nil, fmt.Errorf("process event %s: %w", meta.EventName, err)
		}

		return res, nil
	}

	return nil, fmt.Errorf(
		"unknown [field -> eventType] in message: %s: %w, body: %s", meta.EventName, ErrUnknownEventType, message,
	)
}

type EventProcessor[T eventstream.Event] struct{}

func NewEventProcessor[T eventstream.Event]() *EventProcessor[T] {
	return &EventProcessor[T]{}
}

func (*EventProcessor[T]) Adapt(event eventstream.Event) (any, error) {
	return json.Marshal(event)
}

func (*EventProcessor[T]) ReverseAdapt(message []byte) (eventstream.Event, error) {
	var msg T
	err := json.Unmarshal(message, &msg)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal event: %w", err)
	}

	return msg, nil
}
