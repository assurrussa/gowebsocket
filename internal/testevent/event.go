// Package testevent contains fixtures shared by integration and regression tests.
package testevent

import (
	"errors"

	"github.com/assurrussa/gowebsocket/eventstream"
)

type Event struct {
	ID     eventstream.EventID `json:"eventId"`
	Type   string              `json:"eventType"`
	Body   string              `json:"body"`
	UserID eventstream.UserID  `json:"userId"`
}

func New(body string) *Event                  { return &Event{ID: eventstream.NewEventID(), Type: "test", Body: body} }
func (e *Event) EventID() eventstream.EventID { return e.ID }
func (e *Event) EventName() string            { return e.Type }
func (e *Event) Validate() error {
	if e == nil || e.ID.IsZero() || e.Type == "" || e.Body == "" {
		return errors.New("missing required event fields")
	}
	return nil
}
