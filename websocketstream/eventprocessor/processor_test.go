package eventprocessor_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	logger "github.com/assurrussa/gologger"
	"github.com/assurrussa/goshared/pkg/sharedtypes"
	"github.com/assurrussa/goshared/pkg/tests"
	"github.com/assurrussa/goshared/pkg/validator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"

	eventstream "github.com/assurrussa/gowebsocket/eventstream"
	eventprocessor2 "github.com/assurrussa/gowebsocket/websocketstream/eventprocessor"
	eventprocessormocks "github.com/assurrussa/gowebsocket/websocketstream/eventprocessor/mocks"
)

type TestSuite struct {
	suite.Suite

	ctrl                *gomock.Controller
	eventProcessorMock  *eventprocessormocks.MockEventProcessor
	eventProcessorMock2 *eventprocessormocks.MockEventProcessor

	bf        *bytes.Buffer
	processor *eventprocessor2.Processor
}

func NewTestSuite(t *testing.T) (context.Context, context.CancelFunc, *TestSuite) {
	t.Helper()
	return tests.NewSuite[*TestSuite](t, func(t *testing.T, _ context.Context) *TestSuite {
		t.Helper()
		bf := bytes.NewBuffer(nil)

		ctrl := gomock.NewController(t)
		eventProcessorMock := eventprocessormocks.NewMockEventProcessor(ctrl)
		eventProcessorMock2 := eventprocessormocks.NewMockEventProcessor(ctrl)

		prs := map[string]eventprocessor2.EventProcessor{
			testEventName1: eventProcessorMock,
			testEventName2: eventProcessorMock2,
		}

		processorService, err := eventprocessor2.NewProcessor(eventprocessor2.NewOptions(
			logger.DiscardJSONWithWriter(bf),
			eventprocessor2.WithProcessors(prs),
			eventprocessor2.WithMaxTimeWait(time.Millisecond*100),
		))
		require.NoError(t, err)

		return &TestSuite{
			bf:                  bf,
			ctrl:                ctrl,
			eventProcessorMock:  eventProcessorMock,
			eventProcessorMock2: eventProcessorMock2,
			processor:           processorService,
		}
	})
}

func Test_Init(t *testing.T) {
	assert.NotPanics(t, func() {
		p, err := eventprocessor2.NewProcessor(eventprocessor2.NewOptions(nil))
		require.Error(t, err)
		assert.Nil(t, p)
	})
}

func TestSimpleSubscription(t *testing.T) {
	// Arrange.
	ctx, cancel, ts := NewTestSuite(t)
	defer cancel()

	ts.eventProcessorMock.EXPECT().Handle(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ eventstream.Event) error {
			return nil
		}).Times(2)

	ts.eventProcessorMock2.EXPECT().Handle(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ eventstream.Event) error {
			return errors.New("error expected")
		}).Times(1)

	event := newTestEvent("test body 1")
	ts.processor.Process(ctx, event)

	event2 := newTestEvent2("test body 2")
	ts.processor.Process(ctx, event2)

	event = newTestEvent("test body 3")
	ts.processor.Process(ctx, event)

	go func() {
		for i := 0; i < 5; i++ {
			event = newTestEvent("test body 3")
			ts.processor.Process(ctx, event)
		}
	}()
	ts.processor.Close()

	body := ts.bf.String()
	ts.NotEmpty(body)
}

const (
	testEventName1 = "testEvent"
	testEventName2 = "testEvent2"
)

type testEvent struct {
	ID          sharedtypes.EventID `validate:"required"`
	MessageBody string              `validate:"required,max=3000"`
}

func (t *testEvent) EventID() sharedtypes.EventID {
	return t.ID
}

func (t *testEvent) EventName() string {
	return testEventName1
}

func (t *testEvent) Validate() error {
	return validator.Validator.Struct(t)
}

func newTestEvent(body string) eventstream.Event {
	return &testEvent{
		ID:          sharedtypes.NewEventID(),
		MessageBody: body,
	}
}

type testEvent2 struct {
	ID          sharedtypes.EventID `validate:"required"`
	MessageBody string              `validate:"required,max=3000"`
}

func (t *testEvent2) EventID() sharedtypes.EventID {
	return t.ID
}

func (t *testEvent2) EventName() string {
	return testEventName2
}

func (t *testEvent2) Validate() error {
	return validator.Validator.Struct(t)
}

func newTestEvent2(body string) eventstream.Event {
	return &testEvent2{
		ID:          sharedtypes.NewEventID(),
		MessageBody: body,
	}
}
