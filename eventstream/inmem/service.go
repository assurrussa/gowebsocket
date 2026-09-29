package inmemeventstream

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	logger "github.com/assurrussa/gologger"
	"github.com/imkira/go-observer"

	eventstream "github.com/assurrussa/gowebsocket/eventstream"
)

const serviceName = "event-stream"

// type subscriber struct {
//	ch   chan eventstream.Event
//	done <-chan struct{} // user ctx.Done()
// }
//
// func (sub subscriber) isOnline() bool {
//	select {
//	case <-sub.done:
//		return false
//	default:
//		return true
//	}
// }

type Service struct {
	wg        sync.WaitGroup
	mu        sync.RWMutex
	subs      map[eventstream.UserID]observer.Property
	subsCount map[eventstream.UserID]int
	logger    logger.Logger
}

func New() *Service {
	return &Service{
		wg:        sync.WaitGroup{},
		mu:        sync.RWMutex{},
		subs:      map[eventstream.UserID]observer.Property{},
		subsCount: map[eventstream.UserID]int{},
		logger:    logger.Default().WithNamed(serviceName),
	}
}

func (s *Service) Subscribe(ctx context.Context, userID eventstream.UserID) (<-chan eventstream.Event, error) {
	s.mu.Lock()
	p, ok := s.subs[userID]
	if !ok {
		p = observer.NewProperty(nil)
		s.subs[userID] = p
	}

	s.subsCount[userID]++
	s.mu.Unlock()

	stream := p.Observe()
	events := make(chan eventstream.Event)

	s.wg.Add(1)
	go func() {
		defer func() {
			s.mu.Lock()
			s.subsCount[userID]--
			s.mu.Unlock()

			close(events)
			s.wg.Done()
		}()

		for {
			select {
			case <-ctx.Done():
				return

			case <-stream.Changes():
				select {
				case <-ctx.Done():
					return
				default:
				}

				// ensure safe type assertion
				if ev, ok := stream.Next().(eventstream.Event); ok {
					select {
					case <-ctx.Done():
						return
					case events <- ev:
					}
				}
			}
		}
	}()

	return events, nil
}

func (s *Service) Publish(ctx context.Context, userID eventstream.UserID, event eventstream.Event) error {
	if userID.IsZero() {
		return nil
	}

	if err := event.Validate(); err != nil {
		return fmt.Errorf("invalid event: %w", err)
	}

	if v := s.getSubsCount(userID); v == 0 {
		s.logger.DebugContext(ctx, "no subscribers", slog.String("user_id", userID.String()))

		return nil
	}

	s.mu.RLock()
	p := s.subs[userID]
	s.mu.RUnlock()

	p.Update(event)
	return nil
}

func (s *Service) Close() error {
	s.wg.Wait()
	return nil
}

func (s *Service) getSubsCount(uid eventstream.UserID) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.subsCount[uid]
}
