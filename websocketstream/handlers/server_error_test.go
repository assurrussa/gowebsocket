package handlers_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	libwebsocket "github.com/fasthttp/websocket"

	"github.com/assurrussa/gowebsocket/eventstream"
	"github.com/assurrussa/gowebsocket/internal/testevent"
	"github.com/assurrussa/gowebsocket/websocketstream/eventadapter"
	"github.com/assurrussa/gowebsocket/websocketstream/eventprocessor"
	"github.com/assurrussa/gowebsocket/websocketstream/handlers"
)

type panickingAdapter struct{}

func (panickingAdapter) Adapt(eventstream.Event) (any, error) {
	panic("server adapter failed")
}

func (panickingAdapter) ReverseAdapt([]byte) (eventstream.Event, error) {
	panic("server adapter failed")
}

type panickingDecodeEvent struct{ testevent.Event }

func (*panickingDecodeEvent) UnmarshalJSON([]byte) error { panic("server decoder failed") }

type panickingValidateEvent struct{ testevent.Event }

func (*panickingValidateEvent) Validate() error { panic("server validation failed") }

func TestServerFailuresAreNotClientPolicyViolations(t *testing.T) {
	forTransports(t, func(t *testing.T, newServer serverFactory) {
		t.Helper()
		eventName := testevent.New("type").EventName()
		tests := []struct {
			name   string
			option handlers.OptOptionsSetter
		}{
			{
				name:   "registered adapter panic",
				option: handlers.WithEventAdapters(map[string]eventadapter.EventAdapter{eventName: panickingAdapter{}}),
			},
			{
				name: "nested JSON decoder panic",
				option: handlers.WithEventAdapters(map[string]eventadapter.EventAdapter{
					eventName: eventadapter.NewEventProcessor[*panickingDecodeEvent](),
				}),
			},
			{
				name: "nested event validation panic",
				option: handlers.WithEventAdapters(map[string]eventadapter.EventAdapter{
					eventName: eventadapter.NewEventProcessor[*panickingValidateEvent](),
				}),
			},
			{
				name:   "closed processor",
				option: handlers.WithReadEventProcessor(failingSubmitter{err: eventprocessor.ErrClosed}),
			},
			{
				name:   "wrapped closed processor",
				option: handlers.WithReadEventProcessor(failingSubmitter{err: fmt.Errorf("submit: %w", eventprocessor.ErrClosed)}),
			},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				s := newServer(t, tc.option)
				conn := s.dial(t)
				if err := conn.WriteJSON(testevent.New("valid message")); err != nil {
					t.Fatal(err)
				}
				_, _, err := conn.ReadMessage()
				if !libwebsocket.IsCloseError(err, libwebsocket.CloseInternalServerErr) {
					t.Errorf("server-side failure must close with 1011, got: %v", err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := s.h.Shutdown(ctx); err != nil {
					t.Fatal(err)
				}
				stats := s.h.Stats()
				if stats.PolicyClosed != 0 || stats.InternalClosed != 1 {
					t.Errorf("server-side failure attributed to client policy: %+v", stats)
				}
			})
		}
	})
}
