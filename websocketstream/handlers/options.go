package handlers

import (
	"context"
	"errors"
	"maps"
	"time"

	"github.com/gofiber/fiber/v3"

	"github.com/assurrussa/gowebsocket/eventstream"
	"github.com/assurrussa/gowebsocket/internal/safety"
	"github.com/assurrussa/gowebsocket/internal/wire"
	"github.com/assurrussa/gowebsocket/websocketstream"
	"github.com/assurrussa/gowebsocket/websocketstream/eventadapter"
	"github.com/assurrussa/gowebsocket/websocketstream/eventprocessor"
)

type eventStream interface {
	Subscribe(context.Context, eventstream.UserID) (<-chan eventstream.Event, error)
}

type UserIDExtractor func(fiber.Ctx) (eventstream.UserID, error)
type WireFormat = wire.Format

const (
	LegacyBase64 = wire.LegacyBase64
	JSON         = wire.JSON
)

type Options struct {
	pingPeriod, pongTimeout, writeTimeout, closeTimeout, processTimeout, handoffTimeout time.Duration
	maxMessageBytes, maxDecodedBytes, maxOutboundBytes                                  int64
	maxConnections                                                                      int
	wireFormat                                                                          WireFormat
	logger                                                                              eventstream.Logger
	eventStream                                                                         eventStream
	upgrader                                                                            websocketstream.Upgrader
	shutdownCh                                                                          <-chan struct{}
	userIDCtxKey                                                                        string
	userIDExtractor                                                                     UserIDExtractor
	eventProcessors                                                                     map[string]eventprocessor.EventProcessor
	eventAdapters                                                                       map[string]eventadapter.EventAdapter
	readEventProcessor                                                                  websocketstream.ReadEventProcessor
	eventWriter                                                                         websocketstream.EventWriter
	eventAdapter                                                                        websocketstream.EventAdapter
}

type OptOptionsSetter func(*Options)

// NewOptions preserves the existing constructor and defaults to the legacy
// base64(JSON) inbound format. A nil shutdown channel allows explicit Shutdown.
func NewOptions(logger eventstream.Logger, stream eventStream, upgrader websocketstream.Upgrader,
	shutdown <-chan struct{}, userKey string, options ...OptOptionsSetter) Options {
	o := Options{logger: logger, eventStream: stream, upgrader: upgrader, shutdownCh: shutdown,
		userIDCtxKey: userKey, pingPeriod: 10 * time.Second, writeTimeout: 5 * time.Second,
		closeTimeout: time.Second, processTimeout: time.Second, handoffTimeout: 5 * time.Second,
		maxMessageBytes: 96 << 10, maxDecodedBytes: 64 << 10, maxOutboundBytes: 96 << 10,
		maxConnections: 10000, wireFormat: LegacyBase64}
	for _, option := range options {
		if option != nil {
			option(&o)
		}
	}
	return o
}

func WithPingPeriod(v time.Duration) OptOptionsSetter { return func(o *Options) { o.pingPeriod = v } }
func WithPongWait(v time.Duration) OptOptionsSetter   { return func(o *Options) { o.pongTimeout = v } }
func WithWriteTimeout(v time.Duration) OptOptionsSetter {
	return func(o *Options) { o.writeTimeout = v }
}
func WithCloseTimeout(v time.Duration) OptOptionsSetter {
	return func(o *Options) { o.closeTimeout = v }
}
func WithProcessTimeout(v time.Duration) OptOptionsSetter {
	return func(o *Options) { o.processTimeout = v }
}
func WithHandoffTimeout(v time.Duration) OptOptionsSetter {
	return func(o *Options) { o.handoffTimeout = v }
}
func WithMessageLimits(wireBytes, decodedBytes int64) OptOptionsSetter {
	return func(o *Options) { o.maxMessageBytes, o.maxDecodedBytes = wireBytes, decodedBytes }
}
func WithMaxOutboundBytes(v int64) OptOptionsSetter {
	return func(o *Options) { o.maxOutboundBytes = v }
}
func WithMaxConnections(v int) OptOptionsSetter    { return func(o *Options) { o.maxConnections = v } }
func WithWireFormat(v WireFormat) OptOptionsSetter { return func(o *Options) { o.wireFormat = v } }
func WithUserIDExtractor(v UserIDExtractor) OptOptionsSetter {
	return func(o *Options) { o.userIDExtractor = v }
}
func WithEventProcessors(v map[string]eventprocessor.EventProcessor) OptOptionsSetter {
	return func(o *Options) { o.eventProcessors = maps.Clone(v) }
}
func WithEventAdapters(v map[string]eventadapter.EventAdapter) OptOptionsSetter {
	return func(o *Options) { o.eventAdapters = maps.Clone(v) }
}
func WithReadEventProcessor(v websocketstream.ReadEventProcessor) OptOptionsSetter {
	return func(o *Options) { o.readEventProcessor = v }
}
func WithEventWriter(v websocketstream.EventWriter) OptOptionsSetter {
	return func(o *Options) { o.eventWriter = v }
}
func WithEventAdapter(v websocketstream.EventAdapter) OptOptionsSetter {
	return func(o *Options) { o.eventAdapter = v }
}

func (o *Options) Validate() error {
	if safety.IsNil(o.logger) || safety.IsNil(o.eventStream) || safety.IsNil(o.upgrader) {
		return errors.New("logger, event stream and upgrader are required")
	}
	if o.userIDExtractor == nil && o.userIDCtxKey == "" {
		return errors.New("user extractor or context key required")
	}
	if o.pingPeriod < 100*time.Millisecond || o.pingPeriod > 30*time.Second ||
		o.writeTimeout <= 0 || o.closeTimeout <= 0 || o.processTimeout <= 0 || o.handoffTimeout <= 0 ||
		o.pongTimeout < 0 || (o.pongTimeout > 0 && o.pongTimeout <= o.pingPeriod) {
		return errors.New("invalid ping, pong or operation timeout")
	}
	const maxLimit = 64 << 20
	if o.maxMessageBytes <= 0 || o.maxDecodedBytes <= 0 || o.maxOutboundBytes <= 0 ||
		o.maxMessageBytes > maxLimit || o.maxDecodedBytes > maxLimit || o.maxOutboundBytes > maxLimit || o.maxConnections <= 0 {
		return errors.New("invalid connection or message limits (messages must be <= 64 MiB)")
	}
	if o.wireFormat != LegacyBase64 && o.wireFormat != JSON {
		return errors.New("unsupported wire format")
	}
	for name, handler := range o.eventProcessors {
		if name == "" || safety.IsNil(handler) {
			return errors.New("invalid processor")
		}
	}
	for name, adapter := range o.eventAdapters {
		if name == "" || safety.IsNil(adapter) {
			return errors.New("invalid adapter")
		}
	}
	for _, value := range []any{o.readEventProcessor, o.eventWriter, o.eventAdapter} {
		if value != nil && safety.IsNil(value) {
			return errors.New("typed nil callback")
		}
	}
	return nil
}
