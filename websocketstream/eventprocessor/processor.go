package eventprocessor

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	logger "github.com/assurrussa/gologger"

	eventstream "github.com/assurrussa/gowebsocket/eventstream"
)

//go:generate toolsmocks

const name = "event-client-processor"

type EventProcessor interface {
	Handle(ctx context.Context, req eventstream.Event) error
}

//go:generate options-gen -out-filename=processor_options.gen.go -from-struct=Options
type Options struct {
	logger      logger.Logger `option:"mandatory" validate:"required"`
	processors  map[string]EventProcessor
	maxTimeWait time.Duration `default:"1s"`
}

type Processor struct {
	Options
	wg     *sync.WaitGroup
	closed atomic.Bool
}

func NewProcessor(opts Options) (*Processor, error) {
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("validate options: %w", err)
	}

	opts.logger = opts.logger.WithNamed(name)

	return &Processor{
		Options: opts,
		wg:      &sync.WaitGroup{},
		closed:  atomic.Bool{},
	}, nil
}

func (h *Processor) Process(ctx context.Context, event eventstream.Event) {
	if h.closed.Load() {
		return
	}

	if processor, ok := h.processors[event.EventName()]; ok {
		h.wg.Add(1)
		go h.processHandle(ctx, processor, event)
	}
}

func (h *Processor) Close() {
	h.closed.Store(true)

	h.wg.Wait()
}

func (h *Processor) processHandle(ctx context.Context, processor EventProcessor, event eventstream.Event) {
	defer h.wg.Done()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), h.maxTimeWait)
	defer cancel()

	if err := processor.Handle(ctx, event); err != nil {
		h.logger.ErrorContext(ctx, "error event process handle", logger.Error(err))
	}
}
