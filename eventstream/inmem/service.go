// Package inmemeventstream provides bounded, process-local event fanout.
package inmemeventstream

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/assurrussa/gowebsocket/eventstream"
	"github.com/assurrussa/gowebsocket/internal/bounded"
	"github.com/assurrussa/gowebsocket/internal/eventsnapshot"
)

var (
	ErrClosed          = errors.New("event stream closed")
	ErrSlowConsumer    = errors.New("slow subscriber disconnected; other subscribers may have received the event")
	ErrSubscriberLimit = errors.New("subscriber limit exceeded")
)

// Config limits queued messages and serialized bytes per subscription. One
// additional decoded event may be in flight per subscriber. Limits are not RSS
// limits; custom JSON decoders can allocate more than their input size.
type Config struct {
	QueueCapacity, QueueBytes, MaxEventBytes int
	MaxSubscribers, MaxSubscribersPerUser    int
}

func DefaultConfig() Config {
	return Config{QueueCapacity: 64, QueueBytes: 1 << 20, MaxEventBytes: 64 << 10,
		MaxSubscribers: 10000, MaxSubscribersPerUser: 16}
}

type subscriber struct {
	queue  *bounded.Queue[eventsnapshot.Snapshot]
	cancel context.CancelFunc
}

type Service struct {
	mu                            sync.Mutex
	wg                            sync.WaitGroup
	subs                          map[eventstream.UserID]map[*subscriber]struct{}
	count                         int
	cfg                           Config
	closed                        bool
	done                          chan struct{}
	published, delivered, evicted atomic.Uint64
}

func New() *Service {
	service, _ := NewWithConfig(DefaultConfig()) // Static defaults are valid.
	return service
}

func NewWithConfig(cfg Config) (*Service, error) {
	if cfg.QueueCapacity <= 0 || cfg.QueueBytes <= 0 || cfg.MaxEventBytes <= 0 ||
		cfg.MaxEventBytes > cfg.QueueBytes || cfg.MaxSubscribers <= 0 || cfg.MaxSubscribersPerUser <= 0 {
		return nil, errors.New("positive subscriber/queue limits and MaxEventBytes <= QueueBytes required")
	}
	return &Service{cfg: cfg, subs: make(map[eventstream.UserID]map[*subscriber]struct{}), done: make(chan struct{})}, nil
}

func (s *Service) Subscribe(ctx context.Context, userID eventstream.UserID) (<-chan eventstream.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := userID.Validate(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	if s.count >= s.cfg.MaxSubscribers || len(s.subs[userID]) >= s.cfg.MaxSubscribersPerUser {
		return nil, ErrSubscriberLimit
	}
	q, err := bounded.New[eventsnapshot.Snapshot](s.cfg.QueueCapacity, s.cfg.QueueBytes)
	if err != nil {
		return nil, err
	}
	subCtx, cancel := context.WithCancel(ctx)
	sub := &subscriber{queue: q, cancel: cancel}
	if s.subs[userID] == nil {
		s.subs[userID] = make(map[*subscriber]struct{})
	}
	s.subs[userID][sub] = struct{}{}
	s.count++
	out := make(chan eventstream.Event)
	// Registration is serialized with Shutdown. Wait can never miss a new Add.
	s.wg.Add(1)
	go s.forward(subCtx, userID, sub, out)
	return out, nil
}

func (s *Service) removeLocked(userID eventstream.UserID, sub *subscriber) {
	group := s.subs[userID]
	if _, ok := group[sub]; ok {
		delete(group, sub)
		s.count--
		if len(group) == 0 {
			delete(s.subs, userID)
		}
	}
	sub.cancel()
	sub.queue.Abort()
}

func (s *Service) forward(ctx context.Context, userID eventstream.UserID, sub *subscriber, out chan eventstream.Event) {
	defer s.wg.Done()
	defer close(out)
	defer func() { s.mu.Lock(); s.removeLocked(userID, sub); s.mu.Unlock() }()
	for {
		snapshot, ok := sub.queue.Pop(ctx)
		if !ok {
			return
		}
		event, err := snapshot.Event()
		if err != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case out <- event:
			s.delivered.Add(1)
		}
	}
}

// Publish snapshots the event before returning. Consumers receive independent
// JSON-decoded values. The caller must not mutate the input during Publish.
// Overflow evicts only slow subscribers; retrying ErrSlowConsumer can duplicate
// delivery to healthy subscribers and is not automatically recommended.
func (s *Service) Publish(ctx context.Context, userID eventstream.UserID, event eventstream.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := userID.Validate(); err != nil {
		return err
	}
	snapshot, err := eventsnapshot.New(event, s.cfg.MaxEventBytes)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.published.Add(1)
	var result error
	for sub := range s.subs[userID] {
		if err := sub.queue.TryPush(snapshot, snapshot.Bytes()); err != nil {
			s.removeLocked(userID, sub)
			s.evicted.Add(1)
			result = ErrSlowConsumer
		}
	}
	return result
}

// Shutdown cancels subscribers, discards pending delivery and waits for cleanup.
// It is terminal and idempotent; subsequent Publish/Subscribe return ErrClosed.
func (s *Service) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		for uid, group := range s.subs {
			for sub := range group {
				s.removeLocked(uid, sub)
			}
		}
		go func() { s.wg.Wait(); close(s.done) }()
	}
	s.mu.Unlock()
	select {
	case <-s.done:
		return nil
	default:
	}
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) Close() error { return s.Shutdown(context.Background()) }

type Stats struct {
	Users, Subscribers, QueuedMessages, QueuedBytes int
	Published, Delivered, Evicted                   uint64
}

func (s *Service) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	stats := Stats{Users: len(s.subs), Subscribers: s.count, Published: s.published.Load(),
		Delivered: s.delivered.Load(), Evicted: s.evicted.Load()}
	for _, group := range s.subs {
		for sub := range group {
			n, b := sub.queue.Stats()
			stats.QueuedMessages += n
			stats.QueuedBytes += b
		}
	}
	return stats
}
