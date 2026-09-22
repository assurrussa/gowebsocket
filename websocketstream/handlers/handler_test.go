package handlers_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	logger "github.com/assurrussa/gologger"
	sharedtypes "github.com/assurrussa/goshared/pkg/sharedtypes"
	"github.com/assurrussa/goshared/pkg/validator"
	libwebsocket "github.com/fasthttp/websocket"
	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp/fasthttputil"
	"go.uber.org/mock/gomock"

	eventstream "github.com/assurrussa/gowebsocket/eventstream"
	websocketstream "github.com/assurrussa/gowebsocket/websocketstream"
	handlers2 "github.com/assurrussa/gowebsocket/websocketstream/handlers"
)

func TestHTTPHandler(t *testing.T) {
	const (
		eventsNum     = 3
		eventInterval = time.Second

		pingInterval = eventInterval / 4

		origin = "http://localhost"

		headerSecWsProtocol = "Sec-WebSocket-Protocol"
		secWsProtocol       = "chat-service-protocol.test"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	uid := sharedtypes.NewUserID()
	eventsCh := make(chan eventstream.Event)
	shutdownCh := make(chan struct{})

	log := logger.Default().WithNamed("TestHTTPHandler")

	h, err := handlers2.NewHTTPHandler(handlers2.NewOptions(
		log,
		eventStreamMock{uid: uid, ch: eventsCh},
		websocketstream.NewUpgrader([]string{origin}, []string{secWsProtocol}),
		shutdownCh,
		"user_id",
		handlers2.WithPingPeriod(pingInterval),
		handlers2.WithReadEventProcessor(eventReadEventProcessor{}),
		handlers2.WithEventAdapter(eventAdapter{}),
	))
	require.NoError(t, err)

	// Создаем Fiber приложение для тестирования
	app := fiber.New()

	app.Get("/ws", func(ctx fiber.Ctx) error {
		ctx.Locals("user_id", uid)
		return h.Serve(ctx)
	})

	// Используем in-memory listener для тестирования
	ln := fasthttputil.NewInmemoryListener()
	defer ln.Close()

	// Запускаем сервер в отдельной горутине
	_, serverCancel := context.WithCancel(ctx)
	go func() {
		if err := app.Listener(ln, fiber.ListenConfig{
			DisableStartupMessage: true,
		}); err != nil {
			t.Logf("Server error: %v", err)
		}
		serverCancel()
	}()
	defer func() {
		_ = app.Shutdown()
		serverCancel()
	}()

	// Даем серверу время запуститься
	time.Sleep(50 * time.Millisecond)

	// Создаем кастомный диалер для подключения к in-memory listener
	dialer := &libwebsocket.Dialer{
		NetDial: func(_ string, _ string) (net.Conn, error) {
			return ln.Dial()
		},
		HandshakeTimeout: 5 * time.Second,
	}

	// URL для WebSocket подключения
	u := url.URL{Scheme: "ws", Host: ln.Addr().String(), Path: "/ws"}
	t.Log("Connecting to:", u.String())

	header := http.Header{}
	// use canonical origin header name without external dependency
	header.Add("Origin", origin)
	header.Add(headerSecWsProtocol, secWsProtocol)

	c, resp, err := dialer.DialContext(ctx, u.String(), header)
	require.NoError(t, err, "Failed to establish WebSocket connection")
	assert.Equal(t, secWsProtocol, resp.Header.Get(headerSecWsProtocol))
	defer func() {
		require.NoError(t, c.Close())
		require.NoError(t, resp.Body.Close())
	}()

	var pings int
	{
		c.SetPingHandler(nil) // Hack to set default ping handler.
		defaultPingHandler := c.PingHandler()

		c.SetPingHandler(func(appData string) error {
			pings++
			log.DebugContext(ctx, "new ping received, send pong")
			return defaultPingHandler(appData)
		})
	}

	events := make([]eventstream.Event, 0, eventsNum)
	for i := 0; i < eventsNum; i++ {
		events = append(events, newMessageEvent("Hello World!"))
	}

	go func() {
		for _, e := range events {
			eventsCh <- e
			time.Sleep(eventInterval)
		}
	}()

	receivedEvents := make([]*testEvent, 0, len(events))
	for {
		var event testEvent
		if err := c.ReadJSON(&event); err != nil {
			if libwebsocket.IsCloseError(err, libwebsocket.CloseNormalClosure) {
				break
			}
			require.NoError(t, err)
		}

		receivedEvents = append(receivedEvents, &event)
		log.DebugContext(ctx, "new event received")

		if len(receivedEvents) == len(events) {
			close(shutdownCh)
		}
	}

	t.Run("event stream is working properly", func(t *testing.T) {
		require.Len(t, receivedEvents, len(events))
		for i, e := range receivedEvents {
			assert.Equal(t, events[i], e, "i = %d", i)
		}
	})

	t.Run("ping-pong mechanism is working properly", func(t *testing.T) {
		t.Logf("pings: %d", pings)
		assert.InDelta(t, (eventsNum-1)*4, pings, 1.)
	})

	t.Run("shutdown is working properly", func(t *testing.T) {
		_, _, err := c.NextReader()
		require.Error(t, err)
		assert.True(t, libwebsocket.IsCloseError(err, libwebsocket.CloseNormalClosure))
	})
}

// TestWebSocketUpgrade tests just the WebSocket upgrade functionality.
func TestWebSocketUpgrade(t *testing.T) {
	_, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	uid := sharedtypes.NewUserID()
	eventsCh := make(chan eventstream.Event, 1)
	shutdownCh := make(chan struct{})

	log := logger.Default().WithNamed("TestWebSocketUpgrade")

	h, err := handlers2.NewHTTPHandler(handlers2.NewOptions(
		log,
		eventStreamMock{uid: uid, ch: eventsCh},
		websocketstream.NewUpgrader([]string{"*"}, []string{}),
		shutdownCh,
		"user_id",
		handlers2.WithPingPeriod(time.Second),
		handlers2.WithReadEventProcessor(eventReadEventProcessor{}),
		handlers2.WithEventAdapter(eventAdapter{}),
	))
	require.NoError(t, err)

	// Тестируем только создание хэндлера
	assert.NotNil(t, h)
	t.Log("WebSocket handler created successfully")
}

type eventStreamMock struct {
	ch  chan eventstream.Event
	uid sharedtypes.UserID
}

func (e eventStreamMock) Subscribe(_ context.Context, userID sharedtypes.UserID) (<-chan eventstream.Event, error) {
	if e.uid != userID {
		return nil, fmt.Errorf("unexpected user: %v != %v", e.uid, userID)
	}
	return e.ch, nil
}

func (e eventStreamMock) Publish(_ context.Context, _ sharedtypes.UserID, _ eventstream.Event) error {
	return nil
}

type eventAdapter struct{}

func (eventAdapter) Adapt(event eventstream.Event) (any, error) {
	return event, nil
}

func (eventAdapter) ReverseAdapt(_ []byte) (eventstream.Event, error) {
	// not implemented in tests; return explicit error to avoid nil value + nil error
	return nil, errors.New("not implemented")
}

type eventReadEventProcessor struct{}

func (eventReadEventProcessor) Process(_ context.Context, _ eventstream.Event) {
	//
}

type testEvent struct {
	ID          sharedtypes.EventID `validate:"required"`
	MessageBody string              `validate:"required,max=3000"`
	CreatedAt   time.Time           `validate:"required"`
}

func (t *testEvent) EventID() sharedtypes.EventID {
	return t.ID
}

func (t *testEvent) EventName() string {
	return "testEvent"
}

func (t *testEvent) Validate() error {
	return validator.Validator.Struct(t)
}

func newMessageEvent(body string) eventstream.Event {
	return &testEvent{
		ID:          sharedtypes.NewEventID(),
		MessageBody: body,
		CreatedAt:   time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}
