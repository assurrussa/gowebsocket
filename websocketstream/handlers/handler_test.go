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
	libwebsocket "github.com/fasthttp/websocket"
	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
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

	uid := eventstream.NewUserID()
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

	uid := eventstream.NewUserID()
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

type customUUIDType uuid.UUID

func (c customUUIDType) String() string {
	return uuid.UUID(c).String()
}

type customSessionWithCustomUUID struct {
	id customUUIDType
}

func (s customSessionWithCustomUUID) GetUUID() customUUIDType {
	return s.id
}

type customSessionWithString struct {
	id string
}

func (s customSessionWithString) GetUUID() string {
	return s.id
}

type customSessionWithUUID struct {
	id uuid.UUID
}

func (s customSessionWithUUID) GetUUID() uuid.UUID {
	return s.id
}

type customSessionWithEventstreamUserID struct {
	id eventstream.UserID
}

type sessionWithStringerAndGetUUID struct {
	userUID   eventstream.UserID
	sessionID string
}

func (s sessionWithStringerAndGetUUID) GetUUID() eventstream.UserID {
	return s.userUID
}

func (s sessionWithStringerAndGetUUID) String() string {
	return s.sessionID
}

type sessionWithStringUUIDAndStringer struct {
	userUID   string
	sessionID string
}

func (s sessionWithStringUUIDAndStringer) GetUUID() string {
	return s.userUID
}

func (s sessionWithStringUUIDAndStringer) String() string {
	return s.sessionID
}

type sessionWithReflectedUUIDAndStringer struct {
	userUID   customUUIDType
	sessionID string
}

func (s sessionWithReflectedUUIDAndStringer) GetUUID() customUUIDType {
	return s.userUID
}

func (s sessionWithReflectedUUIDAndStringer) String() string {
	return s.sessionID
}

func (s customSessionWithEventstreamUserID) GetUUID() eventstream.UserID {
	return s.id
}

func TestGetUserIDVariations(t *testing.T) {
	origin := "http://localhost"
	log := logger.Default().WithNamed("TestGetUserIDVariations")
	uid := eventstream.NewUserID()
	rawUUID := uuid.UUID(uid)
	rawStr := uid.String()

	cases := []struct {
		name       string
		val        any
		expectUser bool
	}{
		{
			name:       "eventstream.UserID",
			val:        uid,
			expectUser: true,
		},
		{
			name:       "uuid.UUID",
			val:        rawUUID,
			expectUser: true,
		},
		{
			name:       "string valid uuid",
			val:        rawStr,
			expectUser: true,
		},
		{
			name:       "[16]byte",
			val:        [16]byte(uid),
			expectUser: true,
		},
		{
			name:       "session with eventstream.UserID",
			val:        customSessionWithEventstreamUserID{id: uid},
			expectUser: true,
		},
		{
			name:       "session with uuid.UUID",
			val:        customSessionWithUUID{id: rawUUID},
			expectUser: true,
		},
		{
			name:       "session with string",
			val:        customSessionWithString{id: rawStr},
			expectUser: true,
		},
		{
			name:       "session with custom Stringer UUID",
			val:        customSessionWithCustomUUID{id: customUUIDType(uid)},
			expectUser: true,
		},
		{
			name: "session implementing both GetUUID and Stringer prefers GetUUID",
			val: sessionWithStringerAndGetUUID{
				userUID:   uid,
				sessionID: uuid.New().String(),
			},
			expectUser: true,
		},
		{
			name: "session with invalid string GetUUID and valid Stringer does not fall back to Stringer",
			val: sessionWithStringUUIDAndStringer{
				userUID:   "not-a-valid-uuid",
				sessionID: uuid.New().String(),
			},
			expectUser: false,
		},
		{
			name: "session with empty string GetUUID and valid Stringer does not fall back to Stringer",
			val: sessionWithStringUUIDAndStringer{
				userUID:   "",
				sessionID: uuid.New().String(),
			},
			expectUser: false,
		},
		{
			name: "session with zero eventstream.UserID GetUUID and valid Stringer does not fall back to Stringer",
			val: sessionWithStringerAndGetUUID{
				userUID:   eventstream.UserIDNil,
				sessionID: uuid.New().String(),
			},
			expectUser: false,
		},
		{
			name: "session with zero reflected GetUUID and valid Stringer does not fall back to Stringer",
			val: sessionWithReflectedUUIDAndStringer{
				userUID:   customUUIDType(uuid.Nil),
				sessionID: uuid.New().String(),
			},
			expectUser: false,
		},
		{
			name:       "direct custom Stringer UUID (e.g. legacy sharedtypes.UserID)",
			val:        customUUIDType(uid),
			expectUser: true,
		},
		{
			name:       "nil value",
			val:        nil,
			expectUser: false,
		},
		{
			name:       "zero UserID",
			val:        eventstream.UserIDNil,
			expectUser: false,
		},
		{
			name:       "invalid string",
			val:        "not-a-uuid",
			expectUser: false,
		},
		{
			name:       "typed nil session with string",
			val:        (*customSessionWithString)(nil),
			expectUser: false,
		},
		{
			name:       "typed nil session with uuid.UUID",
			val:        (*customSessionWithUUID)(nil),
			expectUser: false,
		},
		{
			name:       "typed nil session with eventstream.UserID",
			val:        (*customSessionWithEventstreamUserID)(nil),
			expectUser: false,
		},
		{
			name:       "typed nil session with custom Stringer UUID",
			val:        (*customSessionWithCustomUUID)(nil),
			expectUser: false,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			subscribed := make(chan eventstream.UserID, 1)
			mockES := &flexibleEventStreamMock{
				onSubscribe: func(u eventstream.UserID) {
					subscribed <- u
				},
			}

			shutdownCh := make(chan struct{})
			h, err := handlers2.NewHTTPHandler(handlers2.NewOptions(
				log,
				mockES,
				websocketstream.NewUpgrader([]string{origin}, []string{}),
				shutdownCh,
				"user_key",
				handlers2.WithPingPeriod(time.Second),
				handlers2.WithReadEventProcessor(eventReadEventProcessor{}),
				handlers2.WithEventAdapter(eventAdapter{}),
			))
			require.NoError(t, err)

			app := fiber.New()
			app.Get("/ws", func(c fiber.Ctx) error {
				if tt.val != nil {
					c.Locals("user_key", tt.val)
				}
				return h.Serve(c)
			})

			ln := fasthttputil.NewInmemoryListener()
			defer ln.Close()

			go func() {
				_ = app.Listener(ln, fiber.ListenConfig{DisableStartupMessage: true})
			}()
			defer func() {
				_ = app.Shutdown()
			}()

			time.Sleep(30 * time.Millisecond)

			dialer := &libwebsocket.Dialer{
				NetDial: func(_ string, _ string) (net.Conn, error) {
					return ln.Dial()
				},
				HandshakeTimeout: 3 * time.Second,
			}

			header := http.Header{}
			header.Add("Origin", origin)
			c, resp, dialErr := dialer.DialContext(ctx, "ws://localhost/ws", header)
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
			if dialErr == nil {
				defer c.Close()
			}

			if tt.expectUser {
				select {
				case gotUID := <-subscribed:
					assert.Equal(t, uid, gotUID)
					close(shutdownCh)
				case <-time.After(2 * time.Second):
					t.Fatal("timed out waiting for subscription")
				}
			} else {
				select {
				case gotUID := <-subscribed:
					t.Fatalf("unexpected subscription for uid %v", gotUID)
				case <-time.After(100 * time.Millisecond):
					// Expected no subscription
				}
			}
		})
	}
}

type flexibleEventStreamMock struct {
	onSubscribe func(userID eventstream.UserID)
}

func (e *flexibleEventStreamMock) Subscribe(_ context.Context, userID eventstream.UserID) (<-chan eventstream.Event, error) {
	if e.onSubscribe != nil {
		e.onSubscribe(userID)
	}
	ch := make(chan eventstream.Event)
	return ch, nil
}

func (e *flexibleEventStreamMock) Publish(_ context.Context, _ eventstream.UserID, _ eventstream.Event) error {
	return nil
}

type eventStreamMock struct {
	ch  chan eventstream.Event
	uid eventstream.UserID
}

func (e eventStreamMock) Subscribe(_ context.Context, userID eventstream.UserID) (<-chan eventstream.Event, error) {
	if e.uid != userID {
		return nil, fmt.Errorf("unexpected user: %v != %v", e.uid, userID)
	}
	return e.ch, nil
}

func (e eventStreamMock) Publish(_ context.Context, _ eventstream.UserID, _ eventstream.Event) error {
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
	ID          eventstream.EventID `validate:"required"`
	MessageBody string              `validate:"required,max=3000"`
	CreatedAt   time.Time           `validate:"required"`
}

func (t *testEvent) EventID() eventstream.EventID {
	return t.ID
}

func (t *testEvent) EventName() string {
	return "testEvent"
}

func (t *testEvent) Validate() error {
	if t.ID.IsZero() {
		return errors.New("id is required")
	}
	if t.MessageBody == "" {
		return errors.New("message body is required")
	}
	return nil
}

func newMessageEvent(body string) eventstream.Event {
	return &testEvent{
		ID:          eventstream.NewEventID(),
		MessageBody: body,
		CreatedAt:   time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}
