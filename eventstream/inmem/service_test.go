package inmemeventstream_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/assurrussa/gowebsocket/eventstream"
	inmem "github.com/assurrussa/gowebsocket/eventstream/inmem"
	"github.com/assurrussa/gowebsocket/internal/testevent"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func service(t *testing.T) *inmem.Service {
	t.Helper()
	s := inmem.New()
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}
func receive(t *testing.T, events <-chan eventstream.Event) *testevent.Event {
	t.Helper()
	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("subscription closed unexpectedly")
		}
		typed, ok := event.(*testevent.Event)
		if !ok {
			t.Fatalf("unexpected event %T", event)
		}
		return typed
	case <-time.After(2 * time.Second):
		t.Fatal("event delivery timeout")
		return nil
	}
}
func subscribe(t *testing.T, s *inmem.Service, ctx context.Context, id eventstream.UserID) <-chan eventstream.Event {
	t.Helper()
	events, err := s.Subscribe(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return events
}
func publish(t *testing.T, s *inmem.Service, id eventstream.UserID, body string) {
	t.Helper()
	if err := s.Publish(context.Background(), id, testevent.New(body)); err != nil {
		t.Fatal(err)
	}
}

func TestSimpleSubscription(t *testing.T) {
	s := service(t)
	id := eventstream.NewUserID()
	events := subscribe(t, s, context.Background(), id)
	for _, body := range []string{"Hello", "World", "!"} {
		publish(t, s, id, body)
	}
	for _, expected := range []string{"Hello", "World", "!"} {
		if event := receive(t, events); event.Body != expected {
			t.Fatal(event)
		}
	}
}

func TestSimpleSubscriptionNeedClose(t *testing.T) {
	s := service(t)
	id := eventstream.NewUserID()
	events := subscribe(t, s, context.Background(), id)
	publish(t, s, id, "hello")
	_ = receive(t, events)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-events; ok {
		t.Fatal("closed service delivered an event")
	}
	if _, err := s.Subscribe(context.Background(), id); !errors.Is(err, inmem.ErrClosed) {
		t.Fatal(err)
	}
	if err := s.Publish(context.Background(), id, testevent.New("after close")); !errors.Is(err, inmem.ErrClosed) {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestEventIsMultiplexedToStreams(t *testing.T) {
	s := service(t)
	id := eventstream.NewUserID()
	tabs := []<-chan eventstream.Event{
		subscribe(t, s, context.Background(), id), subscribe(t, s, context.Background(), id), subscribe(t, s, context.Background(), id),
	}
	for i := 0; i < 5; i++ {
		publish(t, s, id, strconv.Itoa(i))
	}
	for _, tab := range tabs {
		for i := 0; i < 5; i++ {
			if receive(t, tab).Body != strconv.Itoa(i) {
				t.Fatal("event order")
			}
		}
	}
}

func TestPublishInvalidEvent(t *testing.T) {
	s := service(t)
	id := eventstream.NewUserID()
	for _, event := range []eventstream.Event{nil, (*testevent.Event)(nil), &testevent.Event{}} {
		if err := s.Publish(context.Background(), id, event); !errors.Is(err, eventstream.ErrInvalidEvent) {
			t.Fatalf("invalid event accepted: %v", err)
		}
	}
}

func TestPublishWithoutSubscribers(t *testing.T) {
	s := service(t)
	id := eventstream.NewUserID()
	publish(t, s, id, "offline")
	ctx, cancel := context.WithCancel(context.Background())
	events := subscribe(t, s, ctx, id)
	cancel()
	for range events {
	}
	publish(t, s, id, "offline again")
	if stats := s.Stats(); stats.Users != 0 || stats.Subscribers != 0 {
		t.Fatal(stats)
	}
}

func TestPublishInDifferentUserStreams(t *testing.T) {
	s := service(t)
	ids := []eventstream.UserID{eventstream.NewUserID(), eventstream.NewUserID(), eventstream.NewUserID()}
	tabs := make([]<-chan eventstream.Event, len(ids))
	for i, id := range ids {
		tabs[i] = subscribe(t, s, context.Background(), id)
	}
	for i, id := range ids {
		publish(t, s, id, strconv.Itoa(i))
	}
	for i, tab := range tabs {
		if receive(t, tab).Body != strconv.Itoa(i) {
			t.Fatal("cross-user delivery")
		}
	}
}

func TestChurnReleasesRegistry(t *testing.T) {
	s := service(t)
	for i := 0; i < 500; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		events := subscribe(t, s, ctx, eventstream.NewUserID())
		cancel()
		for range events {
		}
	}
	if stats := s.Stats(); stats.Users != 0 || stats.Subscribers != 0 || stats.QueuedBytes != 0 {
		t.Fatal(stats)
	}
}

func TestSlowConsumerDoesNotBlockHealthySubscriber(t *testing.T) {
	cfg := inmem.DefaultConfig()
	cfg.QueueCapacity = 1
	s, err := inmem.NewWithConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	id := eventstream.NewUserID()
	slow := subscribe(t, s, context.Background(), id)
	healthy := subscribe(t, s, context.Background(), id)
	evicted := false
	for i := 0; i < 4; i++ {
		err := s.Publish(context.Background(), id, testevent.New(strconv.Itoa(i)))
		if errors.Is(err, inmem.ErrSlowConsumer) {
			evicted = true
		} else if err != nil {
			t.Fatal(err)
		}
		if receive(t, healthy).Body != strconv.Itoa(i) {
			t.Fatal("healthy subscriber lost event")
		}
	}
	if !evicted {
		t.Fatal("unbounded slow subscriber")
	}
	select {
	case _, ok := <-slow:
		if ok {
			for range slow {
			}
		}
	case <-time.After(time.Second):
		t.Fatal("slow subscriber not terminated")
	}
	if stats := s.Stats(); stats.Subscribers != 1 || stats.Evicted != 1 {
		t.Fatal(stats)
	}
}

func TestPublishOwnsSnapshot(t *testing.T) {
	s := service(t)
	id := eventstream.NewUserID()
	first := subscribe(t, s, context.Background(), id)
	second := subscribe(t, s, context.Background(), id)
	input := testevent.New("original")
	if err := s.Publish(context.Background(), id, input); err != nil {
		t.Fatal(err)
	}
	input.Body = "caller changed"
	one := receive(t, first)
	one.Body = "consumer changed"
	if two := receive(t, second); two.Body != "original" {
		t.Fatal("shared mutable payload", two)
	}
}

func TestConcurrentSubscribeShutdown(t *testing.T) {
	for iteration := 0; iteration < 20; iteration++ {
		s := service(t)
		var wg sync.WaitGroup
		wg.Add(8)
		for i := 0; i < 8; i++ {
			go func() {
				defer wg.Done()
				events, err := s.Subscribe(context.Background(), eventstream.NewUserID())
				if err == nil {
					for range events {
					}
				}
			}()
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		wg.Wait()
		if s.Stats().Subscribers != 0 {
			t.Fatal("subscriber survived shutdown")
		}
	}
}

func BenchmarkPublishAndReceive(b *testing.B) {
	s := inmem.New()
	defer s.Close()
	id := eventstream.NewUserID()
	events, err := s.Subscribe(context.Background(), id)
	if err != nil {
		b.Fatal(err)
	}
	event := testevent.New("small notification")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := s.Publish(context.Background(), id, event); err != nil {
			b.Fatal(err)
		}
		<-events
	}
}
