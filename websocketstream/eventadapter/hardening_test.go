package eventadapter_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/assurrussa/gowebsocket/eventstream"
	"github.com/assurrussa/gowebsocket/internal/testevent"
	"github.com/assurrussa/gowebsocket/websocketstream/eventadapter"
)

func TestUntrustedInputValidation(t *testing.T) {
	a, err := eventadapter.NewAdapter(eventadapter.NewOptions(eventadapter.WithProcessors(map[string]eventadapter.EventAdapter{
		"test": eventadapter.NewEventProcessor[*testevent.Event](),
	})))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.ReverseAdapt([]byte(`{"eventType":"test"}`)); err == nil {
		t.Fatal("missing fields accepted")
	}
	_, err = a.ReverseAdapt([]byte(`{"eventType":"unknown","secret":"private-token"}`))
	if !errors.Is(err, eventadapter.ErrUnknownEventType) || strings.Contains(err.Error(), "private-token") {
		t.Fatal(err)
	}
	var event *testevent.Event
	if _, err := a.Adapt(event); err == nil {
		t.Fatal("typed nil event accepted")
	}
}

func TestEnvelopeRoundTripAndMetadata(t *testing.T) {
	codec := eventadapter.NewEnvelopeEventProcessor[*testevent.Event]()
	event := testevent.New("body")
	object, err := codec.Adapt(event)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := codec.ReverseAdapt(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.EventID() != event.EventID() {
		t.Fatal("event id changed")
	}
	envelope, ok := object.(eventadapter.Envelope)
	if !ok {
		t.Fatal("not an envelope")
	}
	envelope.EventID = eventstream.NewEventID()
	raw, err = json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.ReverseAdapt(raw); err == nil {
		t.Fatal("conflicting envelope metadata accepted")
	}
}
