package eventprocessor_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/assurrussa/gowebsocket/eventstream"
	"github.com/assurrussa/gowebsocket/internal/testevent"
	"github.com/assurrussa/gowebsocket/websocketstream/eventprocessor"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

type handlerFunc func(context.Context, eventstream.Event) error

func (f handlerFunc) Handle(ctx context.Context, event eventstream.Event) error { return f(ctx, event) }

const testEventType = "test"

func logger() *slog.Logger { return slog.New(slog.DiscardHandler) }

func Test_Init(t *testing.T) {
	if _, err := eventprocessor.NewProcessor(eventprocessor.NewOptions(nil)); err == nil {
		t.Fatal("nil logger accepted")
	}
	if _, err := eventprocessor.NewProcessor(eventprocessor.NewOptions(logger(), eventprocessor.WithMaxTimeWait(0))); err == nil {
		t.Fatal("zero timeout accepted")
	}
}

func TestSimpleSubscription(t *testing.T) {
	var count atomic.Int64
	handlers := map[string]eventprocessor.EventProcessor{
		testEventType: handlerFunc(func(context.Context, eventstream.Event) error { count.Add(1); return nil }),
	}
	p, err := eventprocessor.NewProcessor(eventprocessor.NewOptions(logger(), eventprocessor.WithProcessors(handlers)))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	handlers[testEventType] = handlerFunc(func(context.Context, eventstream.Event) error { panic("registry must be cloned") })
	for i := 0; i < 3; i++ {
		if err := p.Submit(context.Background(), testevent.New("body")); err != nil {
			t.Fatal(err)
		}
	}
	p.Close()
	if count.Load() != 3 || p.Stats().Panics != 0 {
		t.Fatal(count.Load(), p.Stats())
	}
	if err := p.Submit(context.Background(), testevent.New("after close")); !errors.Is(err, eventprocessor.ErrClosed) {
		t.Fatal(err)
	}
}

func TestInvalidAndUnknownEventsNeverRun(t *testing.T) {
	var calls atomic.Int64
	procMap := map[string]eventprocessor.EventProcessor{
		testEventType: handlerFunc(func(context.Context, eventstream.Event) error { calls.Add(1); return nil }),
	}
	p, err := eventprocessor.NewProcessor(eventprocessor.NewOptions(logger(), eventprocessor.WithProcessors(procMap)))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for _, event := range []eventstream.Event{nil, (*testevent.Event)(nil), &testevent.Event{}} {
		if err := p.Submit(context.Background(), event); err == nil {
			t.Fatal("invalid event admitted")
		}
	}
	unknown := testevent.New("body")
	unknown.Type = "unknown"
	if err := p.Submit(context.Background(), unknown); !errors.Is(err, eventprocessor.ErrUnknownEvent) {
		t.Fatal(err)
	}
	p.Close()
	if calls.Load() != 0 {
		t.Fatal("invalid callback executed")
	}
}

func TestIdentityAndCancellation(t *testing.T) {
	id := eventstream.NewUserID()
	got := make(chan eventstream.UserID, 1)
	started := make(chan struct{})
	procMap := map[string]eventprocessor.EventProcessor{
		testEventType: handlerFunc(func(ctx context.Context, _ eventstream.Event) error {
			uid, ok := eventstream.UserIDFromContext(ctx)
			if ok {
				got <- uid
			}
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}),
	}
	p, err := eventprocessor.NewProcessor(eventprocessor.NewOptions(logger(), eventprocessor.WithProcessors(procMap)))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx, cancel := context.WithCancel(eventstream.WithUserID(context.Background(), id))
	defer cancel()
	if err := p.Submit(ctx, testevent.New("body")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("not started")
	}
	cancel()
	p.Close()
	select {
	case uid := <-got:
		if uid != id {
			t.Fatal(uid)
		}
	default:
		t.Fatal("trusted identity lost")
	}
}

func TestConcurrentClose(t *testing.T) {
	procMap := map[string]eventprocessor.EventProcessor{
		testEventType: handlerFunc(func(context.Context, eventstream.Event) error { return nil }),
	}
	p, err := eventprocessor.NewProcessor(eventprocessor.NewOptions(logger(), eventprocessor.WithProcessors(procMap)))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	var wg sync.WaitGroup
	wg.Add(8)
	for i := 0; i < 8; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_ = p.Submit(context.Background(), testevent.New("body"))
			}
		}()
	}
	p.Close()
	before := p.Stats().Completed
	wg.Wait()
	if p.Stats().Completed != before {
		t.Fatal("processing after Close")
	}
}

type admissionGateEvent struct {
	*testevent.Event
	entered, release chan struct{}
	once             sync.Once
}

func (e *admissionGateEvent) EventName() string {
	if e.entered != nil {
		e.once.Do(func() { close(e.entered); <-e.release })
	}
	return e.Event.EventName()
}

func TestAdmissionPausedAcrossClose(t *testing.T) {
	var calls atomic.Int64
	procMap := map[string]eventprocessor.EventProcessor{
		testEventType: handlerFunc(func(context.Context, eventstream.Event) error { calls.Add(1); return nil }),
	}
	p, err := eventprocessor.NewProcessor(eventprocessor.NewOptions(logger(), eventprocessor.WithProcessors(procMap)))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	event := &admissionGateEvent{Event: testevent.New("body"), entered: make(chan struct{}), release: make(chan struct{})}
	result := make(chan error, 1)
	go func() { result <- p.Submit(context.Background(), event) }()
	select {
	case <-event.entered:
	case <-time.After(time.Second):
		t.Fatal("admission did not reach gate")
	}
	p.Close()
	close(event.release)
	select {
	case err := <-result:
		if !errors.Is(err, eventprocessor.ErrClosed) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("admission remained blocked")
	}
	if calls.Load() != 0 {
		t.Fatal("handler ran after completed Close")
	}
}
