// Package workpool executes a bounded queue using a fixed number of workers.
package workpool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/assurrussa/gowebsocket/internal/bounded"
	"github.com/assurrussa/gowebsocket/internal/safety"
)

type Task struct {
	//nolint:containedctx // Task carries per-job context.
	Context context.Context
	Run     func(context.Context) error
	Bytes   int
}

type Config struct {
	Workers, Capacity, MaxBytes int
	Timeout                     time.Duration
	OnError                     func(error)
}

type Stats struct {
	Queued, QueuedBytes                                                            int
	Active, Accepted, Rejected, Completed, Failed, Panics, Canceled, DurationNanos int64
}

type Pool struct {
	queue *bounded.Queue[Task]
	cfg   Config
	//nolint:containedctx // Pool carries lifecycle context.
	ctx                                                                       context.Context
	cancel                                                                    context.CancelFunc
	done                                                                      chan struct{}
	active, accepted, rejected, completed, failed, panics, canceled, duration atomic.Int64
}

func New(cfg Config) (*Pool, error) {
	if cfg.Workers <= 0 || cfg.Workers > 1024 || cfg.Timeout <= 0 {
		return nil, errors.New("workers must be 1..1024 and timeout must be positive")
	}
	q, err := bounded.New[Task](cfg.Capacity, cfg.MaxBytes)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pool{queue: q, cfg: cfg, ctx: ctx, cancel: cancel, done: make(chan struct{})}
	var wg sync.WaitGroup
	wg.Add(cfg.Workers)
	for i := 0; i < cfg.Workers; i++ {
		go func() { defer wg.Done(); p.worker() }()
	}
	go func() { wg.Wait(); cancel(); close(p.done) }()
	return p, nil
}

// Submit and shutdown are serialized by the queue lock, not by a check/Add pair.
func (p *Pool) Submit(task Task) error {
	if task.Context == nil || task.Run == nil {
		return errors.New("nil task context or callback")
	}
	if err := task.Context.Err(); err != nil {
		return err
	}
	if err := p.queue.TryPush(task, task.Bytes); err != nil {
		p.rejected.Add(1)
		return err
	}
	p.accepted.Add(1)
	return nil
}

func (p *Pool) worker() {
	for {
		task, ok := p.queue.Pop(p.ctx)
		if !ok {
			return
		}
		if task.Context.Err() != nil {
			p.canceled.Add(1)
			continue
		}
		p.execute(task)
	}
}

func (p *Pool) execute(task Task) {
	ctx, cancel := context.WithTimeout(task.Context, p.cfg.Timeout)
	stop := context.AfterFunc(p.ctx, cancel)
	defer func() { stop(); cancel() }()
	p.active.Add(1)
	start := time.Now()
	err := safety.Call(func() error { return task.Run(ctx) })
	p.duration.Add(time.Since(start).Nanoseconds())
	p.active.Add(-1)
	p.completed.Add(1)
	if err != nil {
		p.failed.Add(1)
		if errors.Is(err, safety.ErrPanic) {
			p.panics.Add(1)
		}
		if p.cfg.OnError != nil {
			_ = safety.Call(func() error { p.cfg.OnError(err); return nil })
		}
	}
}

// Shutdown drains accepted work. On deadline it discards pending work and
// cancels active contexts. A non-cooperative callback can outlive this method;
// a later successful Shutdown still waits for that callback to return.
func (p *Pool) Shutdown(ctx context.Context) error {
	p.queue.Seal()
	select {
	case <-p.done:
		return nil
	default:
	}
	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		p.cancel()
		p.queue.Abort()
		return ctx.Err()
	}
}

func (p *Pool) Close() { _ = p.Shutdown(context.Background()) }

func (p *Pool) Closed() bool { return p.queue.Closed() }

func (p *Pool) Stats() Stats {
	n, b := p.queue.Stats()
	return Stats{
		Queued: n, QueuedBytes: b, Active: p.active.Load(), Accepted: p.accepted.Load(),
		Rejected: p.rejected.Load(), Completed: p.completed.Load(), Failed: p.failed.Load(),
		Panics: p.panics.Load(), Canceled: p.canceled.Load(), DurationNanos: p.duration.Load(),
	}
}
