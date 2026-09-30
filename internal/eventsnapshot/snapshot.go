// Package eventsnapshot owns an immutable JSON representation of an event.
package eventsnapshot

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/assurrussa/gowebsocket/eventstream"
	"github.com/assurrussa/gowebsocket/internal/safety"
)

var ErrTooLarge = errors.New("event exceeds byte limit")

// Snapshot retains bytes and a type, never the caller's mutable event graph.
// The byte budget measures encoded payload, not total Go heap usage. Application
// MarshalJSON/UnmarshalJSON/Validate methods must be bounded and deterministic.
type Snapshot struct {
	data []byte
	typ  reflect.Type
	name string
}

func New(event eventstream.Event, maxBytes int) (snapshot Snapshot, err error) {
	if maxBytes <= 0 {
		return snapshot, errors.New("event byte limit must be positive")
	}
	if err := eventstream.ValidateEvent(event); err != nil {
		return snapshot, err
	}
	err = safety.Call(func() error {
		data, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("encode event: %w", err)
		}
		if len(data) > maxBytes {
			return ErrTooLarge
		}
		snapshot = Snapshot{data: data, typ: reflect.TypeOf(event), name: strings.Clone(event.EventName())}
		// Reject events whose required fields disappear during the round trip.
		_, err = snapshot.Event()
		return err
	})
	if err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func (s Snapshot) Bytes() int { return len(s.data) }

func (s Snapshot) Name() string { return s.name }

// Event creates an independent event for each consumer.
func (s Snapshot) Event() (event eventstream.Event, err error) {
	err = safety.Call(func() error {
		var target reflect.Value
		if s.typ.Kind() == reflect.Pointer {
			target = reflect.New(s.typ.Elem())
		} else {
			target = reflect.New(s.typ)
		}
		if err := json.Unmarshal(s.data, target.Interface()); err != nil {
			return fmt.Errorf("decode event: %w", err)
		}
		value := target.Interface()
		if s.typ.Kind() != reflect.Pointer {
			value = target.Elem().Interface()
		}
		var ok bool
		event, ok = value.(eventstream.Event)
		if !ok {
			return eventstream.ErrInvalidEvent
		}
		if err := eventstream.ValidateEvent(event); err != nil {
			return err
		}
		if event.EventName() != s.name {
			return errors.New("event name changed during JSON round trip")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return event, nil
}
