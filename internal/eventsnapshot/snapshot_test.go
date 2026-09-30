package eventsnapshot_test

import (
	"testing"

	"github.com/assurrussa/gowebsocket/eventstream"
	"github.com/assurrussa/gowebsocket/internal/eventsnapshot"
	"github.com/assurrussa/gowebsocket/internal/testevent"
)

type hiddenIDEvent struct {
	ID   eventstream.EventID `json:"-"`
	Body string              `json:"body"`
}

func (e hiddenIDEvent) EventID() eventstream.EventID { return e.ID }
func (hiddenIDEvent) EventName() string              { return "hidden-id" }
func (hiddenIDEvent) Validate() error                { return nil }

func TestSnapshotRejectsLostIdentity(t *testing.T) {
	event := hiddenIDEvent{ID: eventstream.NewEventID(), Body: "payload"}
	if _, err := eventsnapshot.New(event, 1024); err == nil {
		t.Fatal("snapshot silently lost the event ID")
	}
}

func TestSnapshotPreservesIdentityAndOwnership(t *testing.T) {
	original := testevent.New("original")
	for _, input := range []eventstream.Event{original, hiddenIDEvent{Body: "zero ID is preserved"}} {
		snapshot, err := eventsnapshot.New(input, 1024)
		if err != nil {
			t.Fatal(err)
		}
		owned, err := snapshot.Event()
		if err != nil || owned.EventID() != input.EventID() || owned.EventName() != input.EventName() {
			t.Fatal("snapshot metadata changed", err)
		}
	}
	snapshot, err := eventsnapshot.New(original, 1024)
	if err != nil {
		t.Fatal(err)
	}
	original.Body = "mutated"
	owned, err := snapshot.Event()
	if err != nil {
		t.Fatal(err)
	}
	first, ok := owned.(*testevent.Event)
	if !ok || first.Body != "original" {
		t.Fatal("snapshot retained mutable input")
	}
	first.Body = "consumer mutation"
	owned, err = snapshot.Event()
	if err != nil {
		t.Fatal(err)
	}
	second, ok := owned.(*testevent.Event)
	if !ok || second.Body != "original" {
		t.Fatal("snapshot shared mutable output")
	}
}
