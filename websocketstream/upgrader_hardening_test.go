package websocketstream_test

import (
	"net"
	"net/http"
	"testing"
	"time"

	libwebsocket "github.com/fasthttp/websocket"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttputil"

	"github.com/assurrussa/gowebsocket/websocketstream"
)

func callbackConnection(t *testing.T, cfg websocketstream.Config, callback libwebsocket.FastHTTPHandler) *libwebsocket.Conn {
	t.Helper()
	const origin = "https://example.com"
	u, err := websocketstream.NewUpgraderChecked([]string{origin}, nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	listener := fasthttputil.NewInmemoryListener()
	server := &fasthttp.Server{Handler: func(ctx *fasthttp.RequestCtx) { _ = u.UpgradeFastHTTP(ctx, callback) }}
	exited := make(chan struct{})
	go func() { defer close(exited); _ = server.Serve(listener) }()
	t.Cleanup(func() {
		_ = server.Shutdown()
		_ = listener.Close()
		select {
		case <-exited:
		case <-time.After(time.Second):
			t.Error("server did not exit")
		}
	})
	dialer := &libwebsocket.Dialer{
		NetDial: func(string, string) (net.Conn, error) { return listener.Dial() }, HandshakeTimeout: time.Second,
	}
	conn, response, err := dialer.Dial("ws://localhost/ws", http.Header{"Origin": []string{origin}})
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	return conn
}

func TestRecoverHandlerCanRecoverCallbackPanic(t *testing.T) {
	recovered := make(chan any, 1)
	conn := callbackConnection(t, websocketstream.Config{RecoverHandler: func(ws *libwebsocket.Conn) {
		value := recover()
		recovered <- value
		if value != nil {
			_ = ws.WriteControl(libwebsocket.CloseMessage,
				libwebsocket.FormatCloseMessage(libwebsocket.ClosePolicyViolation, ""), time.Now().Add(time.Second))
		}
	}}, func(*libwebsocket.Conn) { panic("callback panic") })
	_, _, err := conn.ReadMessage()
	if !libwebsocket.IsCloseError(err, libwebsocket.ClosePolicyViolation) {
		t.Fatalf("custom recovery did not send its close frame: %v", err)
	}
	select {
	case value := <-recovered:
		if value != "callback panic" {
			t.Fatalf("recovery callback could not recover: %v", value)
		}
	case <-time.After(time.Second):
		t.Fatal("recovery callback was not invoked")
	}
}

func TestRecoverHandlerRunsOnNormalReturn(t *testing.T) {
	recovered := make(chan any, 1)
	conn := callbackConnection(t, websocketstream.Config{RecoverHandler: func(*libwebsocket.Conn) {
		recovered <- recover()
	}}, func(*libwebsocket.Conn) {})
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("callback return did not close the socket")
	}
	select {
	case value := <-recovered:
		if value != nil {
			t.Fatal("panic on a normal callback return")
		}
	case <-time.After(time.Second):
		t.Fatal("deferred recovery callback was not invoked")
	}
}

func TestRecoverHandlerPanicHasSafeFallback(t *testing.T) {
	conn := callbackConnection(t, websocketstream.Config{RecoverHandler: func(*libwebsocket.Conn) {
		panic("recovery callback failed")
	}}, func(*libwebsocket.Conn) { panic("original panic") })
	_, _, err := conn.ReadMessage()
	if !libwebsocket.IsCloseError(err, libwebsocket.CloseInternalServerErr) {
		t.Fatalf("fallback did not close with 1011: %v", err)
	}
}

func TestRecoverHandlerWithoutRecoverUsesSafeFallback(t *testing.T) {
	conn := callbackConnection(t, websocketstream.Config{RecoverHandler: func(*libwebsocket.Conn) {}},
		func(*libwebsocket.Conn) { panic("original panic") })
	_, _, err := conn.ReadMessage()
	if !libwebsocket.IsCloseError(err, libwebsocket.CloseInternalServerErr) {
		t.Fatalf("unrecovered callback panic did not close with 1011: %v", err)
	}
}
