package bounded_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/assurrussa/gowebsocket/internal/bounded"
)

func TestLimitsFIFOAndAbort(t *testing.T) {
	q, err := bounded.New[int](2, 5)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.TryPush(1, 3); err != nil {
		t.Fatal(err)
	}
	if err := q.TryPush(2, 3); !errors.Is(err, bounded.ErrFull) {
		t.Fatalf("expected byte limit: %v", err)
	}
	if err := q.TryPush(2, 2); err != nil {
		t.Fatal(err)
	}
	if err := q.TryPush(3, 0); !errors.Is(err, bounded.ErrFull) {
		t.Fatalf("expected count limit: %v", err)
	}
	q.Seal()
	if err := q.TryPush(4, 0); !errors.Is(err, bounded.ErrClosed) {
		t.Fatal(err)
	}
	for expected := 1; expected <= 2; expected++ {
		got, ok := q.Pop(context.Background())
		if !ok || got != expected {
			t.Fatalf("got %d,%v; want %d,true", got, ok, expected)
		}
	}
	if _, ok := q.Pop(context.Background()); ok {
		t.Fatal("sealed queue should be empty")
	}
	q.Abort()
	if n, b := q.Stats(); n != 0 || b != 0 {
		t.Fatalf("retained %d messages, %d bytes", n, b)
	}
}

func TestConcurrentSeal(t *testing.T) {
	for iteration := 0; iteration < 30; iteration++ {
		q, err := bounded.New[int](512, 512)
		if err != nil {
			t.Fatal(err)
		}
		var accepted, delivered atomic.Int64
		var readers, writers sync.WaitGroup
		readers.Add(4)
		for i := 0; i < 4; i++ {
			go func() {
				defer readers.Done()
				for {
					if _, ok := q.Pop(context.Background()); !ok {
						return
					}
					delivered.Add(1)
				}
			}()
		}
		writers.Add(4)
		for i := 0; i < 4; i++ {
			go func() {
				defer writers.Done()
				for j := 0; j < 100; j++ {
					if q.TryPush(j, 1) == nil {
						accepted.Add(1)
					}
				}
			}()
		}
		q.Seal()
		writers.Wait()
		readers.Wait()
		if accepted.Load() != delivered.Load() {
			t.Fatal("lost admitted entries during seal")
		}
	}
}

func TestAbortAndCanceledPop(t *testing.T) {
	q, err := bounded.New[*int](1, 8)
	if err != nil {
		t.Fatal(err)
	}
	value := 1
	if err := q.TryPush(&value, 8); err != nil {
		t.Fatal(err)
	}
	q.Abort()
	if _, ok := q.Pop(context.Background()); ok {
		t.Fatal("aborted queue delivered an entry")
	}
	q, err = bounded.New[*int](1, 8)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok := q.Pop(ctx); ok {
		t.Fatal("canceled context delivered an entry")
	}
}

func BenchmarkQueueRoundTrip(b *testing.B) {
	q, err := bounded.New[int](64, 4096)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := q.TryPush(i, 8); err != nil {
			b.Fatal(err)
		}
		if _, ok := q.Pop(ctx); !ok {
			b.Fatal("unexpected queue closure")
		}
	}
}
