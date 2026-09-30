package workpool_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/assurrussa/gowebsocket/internal/bounded"
	"github.com/assurrussa/gowebsocket/internal/workpool"
)

func newPool(t *testing.T, capacity int) *workpool.Pool {
	t.Helper()
	p, err := workpool.New(workpool.Config{Workers: 1, Capacity: capacity, MaxBytes: 16, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func TestBoundsAndDeadline(t *testing.T) {
	p := newPool(t, 1)
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	if err := p.Submit(workpool.Task{Context: context.Background(), Bytes: 8, Run: func(context.Context) error {
		close(started)
		<-release
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	<-started
	task16 := workpool.Task{Context: context.Background(), Bytes: 16, Run: func(context.Context) error { return nil }}
	if err := p.Submit(task16); err != nil {
		t.Fatal(err)
	}
	task1 := workpool.Task{Context: context.Background(), Bytes: 1, Run: func(context.Context) error { return nil }}
	if err := p.Submit(task1); !errors.Is(err, bounded.ErrFull) {
		t.Fatalf("expected rejection: %v", err)
	}
	stats := p.Stats()
	if stats.Active != 1 || stats.Queued != 1 || stats.QueuedBytes != 16 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := p.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline: %v", err)
	}
	if stats := p.Stats(); stats.Queued != 0 || stats.QueuedBytes != 0 {
		t.Fatalf("pending work retained: %+v", stats)
	}
	if err := p.Submit(task1); !errors.Is(err, bounded.ErrClosed) {
		t.Fatal(err)
	}
}

func TestPanicContainmentAndFIFO(t *testing.T) {
	p := newPool(t, 8)
	var mu sync.Mutex
	var order []int
	for i := 0; i < 5; i++ {
		i := i
		if err := p.Submit(workpool.Task{Context: context.Background(), Bytes: 1, Run: func(context.Context) error {
			if i == 2 {
				panic("private payload must not escape")
			}
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			return nil
		}}); err != nil {
			t.Fatal(err)
		}
	}
	p.Close()
	mu.Lock()
	defer mu.Unlock()
	expected := []int{0, 1, 3, 4}
	if len(order) != len(expected) {
		t.Fatal(order)
	}
	for i := range order {
		if order[i] != expected[i] {
			t.Fatal(order)
		}
	}
	stats := p.Stats()
	if stats.Panics != 1 || stats.Completed != 5 || stats.Active != 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestConcurrentAdmissionShutdown(t *testing.T) {
	for iteration := 0; iteration < 30; iteration++ {
		p := newPool(t, 128)
		var handled atomic.Int64
		var writers sync.WaitGroup
		writers.Add(4)
		for i := 0; i < 4; i++ {
			go func() {
				defer writers.Done()
				for j := 0; j < 20; j++ {
					task := workpool.Task{Context: context.Background(), Bytes: 0, Run: func(context.Context) error {
						handled.Add(1)
						return nil
					}}
					_ = p.Submit(task)
				}
			}()
		}
		p.Close()
		before := handled.Load()
		writers.Wait()
		if handled.Load() != before {
			t.Fatal("handler ran after completed shutdown")
		}
		if stats := p.Stats(); stats.Active != 0 || stats.Queued != 0 {
			t.Fatal(stats)
		}
	}
}

func TestShutdownCancelsActiveContext(t *testing.T) {
	p := newPool(t, 1)
	started, canceled := make(chan struct{}), make(chan struct{})
	if err := p.Submit(workpool.Task{Context: context.Background(), Bytes: 1, Run: func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		close(canceled)
		return ctx.Err()
	}}); err != nil {
		t.Fatal(err)
	}
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("active context was not canceled")
	}
	p.Close()
}
