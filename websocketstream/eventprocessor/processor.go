// Package eventprocessor routes events through bounded application workers.
package eventprocessor

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/assurrussa/gowebsocket/eventstream"
	"github.com/assurrussa/gowebsocket/internal/bounded"
	"github.com/assurrussa/gowebsocket/internal/eventsnapshot"
	"github.com/assurrussa/gowebsocket/internal/safety"
	"github.com/assurrussa/gowebsocket/internal/workpool"
)

//go:generate go run go.uber.org/mock/mockgen@v0.6.0 -source=processor.go -destination=mocks/processor_mock.gen.go -package=eventprocessormocks

type EventProcessor interface {
	Handle(ctx context.Context, event eventstream.Event) error
}

var (
	ErrClosed       = bounded.ErrClosed
	ErrOverloaded   = bounded.ErrFull
	ErrUnknownEvent = errors.New("unknown incoming event type")
)

type Options struct {
	logger                                            eventstream.Logger
	processors                                        map[string]EventProcessor
	maxTimeWait                                       time.Duration
	workers, queueCapacity, queueBytes, maxEventBytes int
	detach                                            bool
}

type OptOptionsSetter func(*Options)

func NewOptions(logger eventstream.Logger, options ...OptOptionsSetter) Options {
	o := Options{
		logger: logger, maxTimeWait: time.Second, workers: 1,
		queueCapacity: 128, queueBytes: 2 << 20, maxEventBytes: 64 << 10,
	}
	for _, option := range options {
		if option != nil {
			option(&o)
		}
	}
	return o
}

func WithProcessors(value map[string]EventProcessor) OptOptionsSetter {
	return func(o *Options) { o.processors = maps.Clone(value) }
}

func WithMaxTimeWait(value time.Duration) OptOptionsSetter {
	return func(o *Options) { o.maxTimeWait = value }
}
func WithWorkers(value int) OptOptionsSetter { return func(o *Options) { o.workers = value } }
func WithQueueLimits(messages, bytes int) OptOptionsSetter {
	return func(o *Options) { o.queueCapacity, o.queueBytes = messages, bytes }
}

func WithMaxEventBytes(value int) OptOptionsSetter {
	return func(o *Options) { o.maxEventBytes = value }
}

// WithDetachedContext explicitly opts into processing after client disconnect.
// The pool's own shutdown cancellation and per-task timeout still apply.
func WithDetachedContext(value bool) OptOptionsSetter { return func(o *Options) { o.detach = value } }

func (o *Options) Validate() error {
	if safety.IsNil(o.logger) {
		return errors.New("logger is required")
	}
	if o.maxTimeWait <= 0 || o.workers <= 0 || o.workers > 1024 || o.queueCapacity <= 0 ||
		o.queueBytes <= 0 || o.maxEventBytes <= 0 || o.maxEventBytes > o.queueBytes {
		return errors.New("invalid processor timeout, workers or queue limits")
	}
	for name, handler := range o.processors {
		if name == "" || safety.IsNil(handler) {
			return errors.New("invalid processor registration")
		}
	}
	return nil
}

// Processor owns a fixed worker pool. One worker (default) executes FIFO;
// multiple workers deliberately do not guarantee completion order.
type Processor struct {
	Options
	pool *workpool.Pool
}

func NewProcessor(opts Options) (*Processor, error) {
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("validate options: %w", err)
	}
	opts.processors = maps.Clone(opts.processors)
	pool, err := workpool.New(workpool.Config{
		Workers: opts.workers, Capacity: opts.queueCapacity,
		MaxBytes: opts.queueBytes, Timeout: opts.maxTimeWait, OnError: func(err error) {
			// Error strings from domain callbacks may contain secrets. Do not log them.
			opts.logger.ErrorContext(context.Background(), "event handler failed", "panic", errors.Is(err, safety.ErrPanic))
		},
	})
	if err != nil {
		return nil, err
	}
	return &Processor{Options: opts, pool: pool}, nil
}

// Submit reports rejection synchronously. It never waits for worker capacity.
func (p *Processor) Submit(ctx context.Context, event eventstream.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	snapshot, err := eventsnapshot.New(event, p.maxEventBytes)
	if err != nil {
		return err
	}
	handler, ok := p.processors[snapshot.Name()]
	if !ok {
		return ErrUnknownEvent
	}
	if p.detach {
		ctx = context.WithoutCancel(ctx)
	}
	return p.pool.Submit(workpool.Task{Context: ctx, Bytes: snapshot.Bytes(), Run: func(ctx context.Context) error {
		owned, err := snapshot.Event()
		if err != nil {
			return err
		}
		return handler.Handle(ctx, owned)
	}})
}

// Process is the compatibility API. New consumers should use Submit to observe
// overload. HTTPHandler automatically uses Submit when supported.
func (p *Processor) Process(ctx context.Context, event eventstream.Event) {
	if err := p.Submit(ctx, event); err != nil {
		p.logger.WarnContext(ctx, "event rejected")
	}
}

func (p *Processor) Close()                             { p.pool.Close() }
func (p *Processor) Shutdown(ctx context.Context) error { return p.pool.Shutdown(ctx) }

type Stats = workpool.Stats

func (p *Processor) Stats() Stats { return p.pool.Stats() }
