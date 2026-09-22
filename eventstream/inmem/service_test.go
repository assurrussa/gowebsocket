package inmemeventstream_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/assurrussa/goshared/pkg/sharedtypes"
	"github.com/assurrussa/goshared/pkg/tests"
	"github.com/assurrussa/goshared/pkg/validator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"

	eventstream "github.com/assurrussa/gowebsocket/eventstream"
	inmemeventstream "github.com/assurrussa/gowebsocket/eventstream/inmem"
)

var defaultBodies = []string{"Hello", "World", "!"}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type TestSuite struct {
	suite.Suite

	stream eventstream.EventStream
}

func NewTestRepoSuite(t *testing.T) (context.Context, context.CancelFunc, *TestSuite) {
	t.Helper()
	return tests.NewSuite[*TestSuite](t, func(t *testing.T, _ context.Context) *TestSuite {
		t.Helper()
		stream := inmemeventstream.New()
		t.Cleanup(func() {
			assert.NoError(t, stream.Close())
		})

		return &TestSuite{
			stream: stream,
		}
	})
}

func TestSimpleSubscription(t *testing.T) {
	// Arrange.
	ctx, cancel, ts := NewTestRepoSuite(t)
	defer cancel()
	uid := sharedtypes.NewUserID()

	events, err := ts.stream.Subscribe(ctx, uid)
	ts.Require().NoError(err)

	bodies := defaultBodies
	result := readNewMessageEvents(events, len(bodies))

	// Action.
	for _, b := range bodies {
		ts.Require().NoError(ts.stream.Publish(ctx, uid, newMessageEvent(b)))
	}

	// Assert.
	ts.Equal(defaultBodies, <-result)
}

func TestSimpleSubscriptionNeedClose(t *testing.T) {
	// Arrange.
	ctx, cancel, ts := NewTestRepoSuite(t)
	uid := sharedtypes.NewUserID()

	events, err := ts.stream.Subscribe(ctx, uid)
	ts.Require().NoError(err)

	bodies := defaultBodies
	result := readNewMessageEvents(events, len(bodies))

	// Action.
	for _, b := range bodies {
		ts.Require().NoError(ts.stream.Publish(ctx, uid, newMessageEvent(b)))
	}

	// Assert.
	ts.Equal(defaultBodies, <-result)
	cancel()
	ts.Require().NoError(ts.stream.Close())

	// Arrange repeat.
	result = readNewMessageEvents(events, len(bodies))

	// Action.
	for _, b := range bodies {
		ts.Require().NoError(ts.stream.Publish(ctx, uid, newMessageEvent(b)))
	}

	// Assert.
	ts.Nil(<-result)
	ts.Require().NoError(ts.stream.Close())
}

func TestEventIsMultiplexedToStreams(t *testing.T) {
	// Arrange.
	ctx, cancel, ts := NewTestRepoSuite(t)
	defer cancel()
	uid := sharedtypes.NewUserID()

	tab1, err := ts.stream.Subscribe(ctx, uid)
	ts.Require().NoError(err)

	tab2, err := ts.stream.Subscribe(ctx, uid)
	ts.Require().NoError(err)

	tab3, err := ts.stream.Subscribe(ctx, uid)
	ts.Require().NoError(err)

	const (
		tabsCount        = 3
		messagesCount    = 5
		allMessagesCount = tabsCount * messagesCount
	)

	// Action.
	expectedCh := make(chan []string)
	go func() {
		expected := make([]string, 0, allMessagesCount)
		for i := 0; i < messagesCount; i++ {
			v := strconv.Itoa(i)
			err := ts.stream.Publish(ctx, uid, newMessageEvent(v))
			ts.NoError(err)

			for i := 0; i < tabsCount; i++ {
				expected = append(expected, v)
			}
		}
		expectedCh <- expected
	}()

	// Assert.
	msgs := make([]string, 0, allMessagesCount)
	for i := 0; i < allMessagesCount; i++ {
		var event eventstream.Event
		select {
		case event = <-tab1:
		case event = <-tab2:
		case event = <-tab3:
		case <-time.After(time.Second):
			ts.FailNow("lost events")
		}
		msgEv, ok := event.(*testEvent)
		ts.Require().True(ok)
		msgs = append(msgs, msgEv.MessageBody)
	}
	ts.ElementsMatch(<-expectedCh, msgs)
}

func TestPublishInvalidEvent(t *testing.T) {
	// Arrange.
	ctx, cancel, ts := NewTestRepoSuite(t)
	defer cancel()
	uid := sharedtypes.NewUserID()

	events, err := ts.stream.Subscribe(ctx, uid)
	ts.Require().NoError(err)

	// Not filled event.
	err = ts.stream.Publish(ctx, uid, &testEvent{})
	ts.Require().Error(err)

	select {
	case ev := <-events:
		ts.FailNow("unexpected event", ev)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestPublishWithoutSubscribers(t *testing.T) {
	// Arrange.
	ctx, cancel, ts := NewTestRepoSuite(t)
	defer cancel()
	ts.Run("no subscriptions at all", func() {
		err := ts.stream.Publish(ctx, sharedtypes.NewUserID(), newMessageEvent("Hello"))
		ts.Require().NoError(err)
	})

	ts.Run("publish to offline client", func() {
		uid1, uid2 := sharedtypes.NewUserID(), sharedtypes.NewUserID()

		// uid1 is online.
		_, err := ts.stream.Subscribe(ctx, uid1)
		ts.Require().NoError(err)

		// uid2 is offline.
		err = ts.stream.Publish(ctx, uid2, newMessageEvent("No panic"))
		ts.Require().NoError(err)
	})

	ts.Run("client was online and became offline", func() {
		// Arrange.
		uid := sharedtypes.NewUserID()

		subscribe := func(n int) (<-chan []string, context.CancelFunc) {
			ctx, cancel := context.WithCancel(ctx)
			// No cancel().

			tab, err := ts.stream.Subscribe(ctx, uid)
			ts.Require().NoError(err)

			return readNewMessageEvents(tab, n), func() {
				time.Sleep(10 * time.Millisecond)
				cancel()
			}
		}

		publish := func(v string) {
			err := ts.stream.Publish(ctx, uid, newMessageEvent(v))
			ts.Require().NoError(err)
		}

		// Action.
		tab1, cancel1 := subscribe(-1)
		publish("1")

		tab2, cancel2 := subscribe(-1)
		publish("2")

		tab3, cancel3 := subscribe(-1)
		publish("3")

		cancel3()
		publish("4")

		cancel2()
		publish("5")

		cancel1()
		publish("6")

		// Assert.
		ts.Equal([]string{"1", "2", "3", "4", "5"}, <-tab1)
		ts.Equal([]string{"2", "3", "4"}, <-tab2)
		ts.Equal([]string{"3"}, <-tab3)
	})
}

func TestPublishInDifferentUserStreams(t *testing.T) {
	// Arrange.
	ctx, cancel, ts := NewTestRepoSuite(t)
	defer cancel()
	// Arrange.
	const users = 3
	const messagesPerUser = 10

	uids := make([]sharedtypes.UserID, 0, users)
	msgChannels := make([]<-chan []string, 0, users)

	for i := 0; i < users; i++ {
		uid := sharedtypes.NewUserID()

		events, err := ts.stream.Subscribe(ctx, uid)
		ts.Require().NoError(err)

		uids = append(uids, uid)
		msgChannels = append(msgChannels, readNewMessageEvents(events, messagesPerUser))
	}

	// Action.
	expectedMsgs := make([][]string, users)
	for i := 0; i < users; i++ {
		expectedMsgs[i] = make([]string, 0, messagesPerUser)
	}

	for i := 0; i < messagesPerUser; i++ {
		for j := 0; j < users; j++ {
			uid := uids[j]
			v := strconv.Itoa(i*users + j)

			err := ts.stream.Publish(ctx, uid, newMessageEvent(v))
			ts.Require().NoError(err)

			expectedMsgs[j] = append(expectedMsgs[j], v)
		}
	}

	// Assert.
	receivedMsgs := make([][]string, 0, users)
	for _, ch := range msgChannels {
		receivedMsgs = append(receivedMsgs, <-ch)
	}

	ts.T().Log("received events", receivedMsgs)
	ts.Equal(expectedMsgs, receivedMsgs)
}

// readNewMessageEvents reads n events from the stream.
// If n is negative, then the function reads the stream until it is closed.
func readNewMessageEvents(stream <-chan eventstream.Event, n int) <-chan []string {
	result := make(chan []string)
	var msgs []string // No preallocation, n can be negative.
	go func() {
		for ev := range stream {
			msgEv, ok := ev.(*testEvent)
			if !ok {
				continue
			}
			msg := msgEv.MessageBody
			msgs = append(msgs, msg)
			if n != -1 && len(msgs) == n {
				break
			}
		}
		result <- msgs
	}()
	return result
}

type testEvent struct {
	ID          sharedtypes.EventID `validate:"required"`
	MessageBody string              `validate:"required,max=3000"`
	CreatedAt   time.Time           `validate:"required"`
}

func (t *testEvent) EventID() sharedtypes.EventID {
	return t.ID
}

func (t *testEvent) EventName() string {
	return "testEvent"
}

func (t *testEvent) Validate() error {
	return validator.Validator.Struct(t)
}

func newMessageEvent(body string) eventstream.Event {
	return &testEvent{
		ID:          sharedtypes.NewEventID(),
		MessageBody: body,
		CreatedAt:   time.Now(),
	}
}
