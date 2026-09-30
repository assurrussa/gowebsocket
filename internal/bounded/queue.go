// Package bounded implements bounded FIFO mailboxes with explicit shutdown.
package bounded

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrClosed = errors.New("queue closed")
	ErrFull   = errors.New("queue capacity exceeded")
)

type entry[T any] struct {
	value T
	size  int
}

// Queue accounts for queued bytes, excluding values already returned by Pop.
// T must not be mutated after admission. All methods are concurrency-safe.
type Queue[T any] struct {
	mu                           sync.Mutex
	items                        []entry[T]
	head, count, bytes, maxBytes int
	sealed                       bool
	ready                        chan struct{}
	closed                       chan struct{}
}

func New[T any](capacity, maxBytes int) (*Queue[T], error) {
	if capacity <= 0 || maxBytes <= 0 {
		return nil, errors.New("queue limits must be positive")
	}
	return &Queue[T]{items: make([]entry[T], capacity), maxBytes: maxBytes,
		ready: make(chan struct{}, 1), closed: make(chan struct{})}, nil
}

func (q *Queue[T]) signal() {
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

// TryPush never waits for consumer capacity. Size must include the payload bytes.
func (q *Queue[T]) TryPush(value T, size int) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.sealed {
		return ErrClosed
	}
	if size < 0 || size > q.maxBytes-q.bytes || q.count == len(q.items) {
		return ErrFull
	}
	q.items[(q.head+q.count)%len(q.items)] = entry[T]{value: value, size: size}
	q.count++
	q.bytes += size
	q.signal()
	return nil
}

// Pop drains a sealed queue; an aborted queue or canceled context stops at once.
func (q *Queue[T]) Pop(ctx context.Context) (T, bool) {
	var zero T
	for {
		if ctx.Err() != nil {
			return zero, false
		}
		q.mu.Lock()
		if q.count > 0 {
			item := q.items[q.head]
			q.items[q.head] = entry[T]{}
			q.head = (q.head + 1) % len(q.items)
			q.count--
			q.bytes -= item.size
			if q.count > 0 {
				q.signal()
			}
			q.mu.Unlock()
			return item.value, true
		}
		sealed := q.sealed
		q.mu.Unlock()
		if sealed {
			return zero, false
		}
		select {
		case <-ctx.Done():
			return zero, false
		case <-q.ready:
		case <-q.closed:
		}
	}
}

// Seal rejects future producers and lets consumers drain existing entries.
func (q *Queue[T]) Seal() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.sealed {
		q.sealed = true
		close(q.closed)
	}
}

// Abort releases all queued references and rejects future producers.
func (q *Queue[T]) Abort() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.sealed {
		q.sealed = true
		close(q.closed)
	}
	clear(q.items)
	q.head, q.count, q.bytes = 0, 0, 0
}

func (q *Queue[T]) Stats() (messages, bytes int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.count, q.bytes
}
