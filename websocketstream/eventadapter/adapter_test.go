package eventadapter_test

import (
	"errors"
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	eventstream "github.com/assurrussa/gowebsocket/eventstream"
	eventadapter2 "github.com/assurrussa/gowebsocket/websocketstream/eventadapter"
)

const (
	smokeCaseName      = "smoke"
	defaultEventIDText = "d0ffbd36-bc30-11ed-8286-461e464ebed8"
	helloWorldBody     = "hello world"
	defaultEventType   = "TestEventType"
	adaptedEventType   = "TestEventTypeAdapt"
	defaultEventJSON   = `{"body":"hello world", "eventId":"d0ffbd36-bc30-11ed-8286-461e464ebed8", "eventType":"TestEventType"}`
	adaptedEventJSON   = `{"body":"hello world", "eventId":"d0ffbd36-bc30-11ed-8286-461e464ebed8", "eventType":"TestEventTypeAdapt"}`
)

func TestAdapter_Init(t *testing.T) {
	assert.NotPanics(t, func() {
		adapted, err := eventadapter2.NewAdapter(eventadapter2.NewOptions())
		require.NoError(t, err)
		require.NotNil(t, adapted)
	})
}

func TestAdapter_Adapt(t *testing.T) {
	eventID := eventstream.MustParse[eventstream.EventID](defaultEventIDText)
	defaultEventMessage := newMessageEvent(eventID, helloWorldBody)
	defaultEventAdaptMessage := newMessageEventAdapt(eventID, helloWorldBody)
	processors := map[string]eventadapter2.EventAdapter{
		defaultEventMessage.EventName():      &eventAdapter{},
		defaultEventAdaptMessage.EventName(): eventadapter2.NewEventProcessor[*testEventAdapt](),
	}
	cases := []struct {
		name       string
		ev         eventstream.Event
		processors map[string]eventadapter2.EventAdapter
		expJSON    string
		wantErr    bool
	}{
		{
			name:       smokeCaseName,
			ev:         defaultEventMessage,
			processors: processors,
			expJSON:    defaultEventJSON,
			wantErr:    false,
		},
		{
			name:       smokeCaseName,
			ev:         defaultEventAdaptMessage,
			processors: processors,
			expJSON:    adaptedEventJSON,
			wantErr:    false,
		},
		{
			name: smokeCaseName,
			ev:   defaultEventMessage,
			processors: map[string]eventadapter2.EventAdapter{
				defaultEventMessage.EventName(): &eventAdapter{err: errors.New("err test expect")},
			},
			expJSON: defaultEventJSON,
			wantErr: true,
		},
		{
			name:       smokeCaseName,
			ev:         defaultEventMessage,
			processors: map[string]eventadapter2.EventAdapter{},
			wantErr:    true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			adapted, err := eventadapter2.NewAdapter(eventadapter2.NewOptions(
				eventadapter2.WithProcessors(tt.processors),
			))
			require.NoError(t, err)

			raw, err := adapted.Adapt(tt.ev)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, raw)
				return
			}

			require.NoError(t, err)
			body, ok := raw.([]byte)
			require.True(t, ok)
			assert.JSONEq(t, tt.expJSON, string(body))
		})
	}
}

func TestAdapter_AdaptInverse(t *testing.T) {
	eventID := eventstream.MustParse[eventstream.EventID](defaultEventIDText)
	defaultEventMessage := newMessageEvent(eventID, helloWorldBody)
	defaultEventAdaptMessage := newMessageEventAdapt(eventID, helloWorldBody)
	processors := map[string]eventadapter2.EventAdapter{
		defaultEventMessage.EventName():      &eventAdapter{},
		defaultEventAdaptMessage.EventName(): eventadapter2.NewEventProcessor[*testEventAdapt](),
	}

	cases := []struct {
		name       string
		ev         eventstream.Event
		processors map[string]eventadapter2.EventAdapter
		rawMessage string
		wantErr    bool
	}{
		{
			name:       smokeCaseName,
			ev:         defaultEventMessage,
			processors: processors,
			rawMessage: defaultEventJSON,
			wantErr:    false,
		},
		{
			name:       smokeCaseName,
			ev:         defaultEventAdaptMessage,
			processors: processors,
			rawMessage: adaptedEventJSON,
			wantErr:    false,
		},
		{
			name:       "smoke",
			ev:         defaultEventMessage,
			processors: map[string]eventadapter2.EventAdapter{},
			rawMessage: `{"body":"hello world", "eventId":"d0ffbd36-bc30-11ed-8286-461e464ebed8", "eventType":"TestEventType"}`,
			wantErr:    true,
		},
		{
			name: smokeCaseName,
			ev:   defaultEventMessage,
			processors: map[string]eventadapter2.EventAdapter{
				defaultEventMessage.EventName(): &eventAdapter{err: errors.New("err test expect")},
			},
			rawMessage: defaultEventJSON,
			wantErr:    true,
		},
		{
			name:       smokeCaseName,
			ev:         defaultEventMessage,
			processors: map[string]eventadapter2.EventAdapter{},
			wantErr:    true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			adapted, err := eventadapter2.NewAdapter(eventadapter2.NewOptions(
				eventadapter2.WithProcessors(tt.processors),
			))
			require.NoError(t, err)

			ev, err := adapted.ReverseAdapt([]byte(tt.rawMessage))
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, ev)
				return
			}

			assert.IsType(t, tt.ev, ev)
		})
	}
}

type testEvent struct {
	ID          eventstream.EventID `json:"eventId" validate:"required"`
	EventType   string              `json:"eventType" validate:"required"`
	MessageBody string              `json:"body" validate:"required,max=3000"`
}

func (t *testEvent) EventID() eventstream.EventID {
	return t.ID
}

func (t *testEvent) EventName() string {
	return t.EventType
}

func (t *testEvent) Validate() error {
	if t.ID.IsZero() {
		return errors.New("id is required")
	}
	if t.EventType == "" {
		return errors.New("eventType is required")
	}
	if t.MessageBody == "" {
		return errors.New("message body is required")
	}
	return nil
}

func newMessageEvent(id eventstream.EventID, body string) eventstream.Event {
	return &testEvent{
		ID:          id,
		EventType:   defaultEventType,
		MessageBody: body,
	}
}

type testEventAdapt struct {
	ID          eventstream.EventID `json:"eventId" validate:"required"`
	EventType   string              `json:"eventType" validate:"required"`
	MessageBody string              `json:"body" validate:"required,max=3000"`
}

func (t *testEventAdapt) EventID() eventstream.EventID {
	return t.ID
}

func (t *testEventAdapt) EventName() string {
	return t.EventType
}

func (t *testEventAdapt) Validate() error {
	if t.ID.IsZero() {
		return errors.New("id is required")
	}
	if t.EventType == "" {
		return errors.New("eventType is required")
	}
	if t.MessageBody == "" {
		return errors.New("message body is required")
	}
	return nil
}

func newMessageEventAdapt(id eventstream.EventID, body string) eventstream.Event {
	return &testEventAdapt{
		ID:          id,
		EventType:   adaptedEventType,
		MessageBody: body,
	}
}

type eventAdapter struct {
	err error
}

func (adapter *eventAdapter) Adapt(event eventstream.Event) (any, error) {
	if adapter.err != nil {
		return nil, adapter.err
	}

	return json.Marshal(event)
}

func (adapter *eventAdapter) ReverseAdapt(message []byte) (eventstream.Event, error) {
	if adapter.err != nil {
		return nil, adapter.err
	}
	var msg testEvent
	err := json.Unmarshal(message, &msg)
	return &msg, err
}
