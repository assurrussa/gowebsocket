#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
cd "$work"
cat >go.mod <<MOD
module consumer.example/gowebsocket-smoke

go 1.27.0

toolchain go1.27.1

require github.com/assurrussa/gowebsocket ${GOWEBSOCKET_VERSION:-v0.0.0}
MOD
if [[ -z "${GOWEBSOCKET_VERSION:-}" ]]; then
  printf '\nreplace github.com/assurrussa/gowebsocket => %s\n' "$root" >>go.mod
fi
cat >consumer_test.go <<'GO'
package consumer_test
import (
 "context"
 "errors"
 "log/slog"
 "net/http"
 "net/http/httptest"
 "strings"
 "testing"
 "time"

 libwebsocket "github.com/fasthttp/websocket"
 "github.com/assurrussa/gowebsocket/eventstream"
 inmem "github.com/assurrussa/gowebsocket/eventstream/inmem"
 "github.com/assurrussa/gowebsocket/websocketstream"
 "github.com/assurrussa/gowebsocket/websocketstream/eventadapter"
 "github.com/assurrussa/gowebsocket/websocketstream/eventprocessor"
 "github.com/assurrussa/gowebsocket/websocketstream/handlers"
)
type message struct {
 ID eventstream.EventID `json:"eventId"`
 Type string `json:"eventType"`
 Body string `json:"body"`
}
func (m *message) EventID() eventstream.EventID { return m.ID }
func (m *message) EventName() string { return m.Type }
func (m *message) Validate() error {
 if m.ID.IsZero() || m.Type != "echo" || m.Body == "" { return errors.New("invalid message") }
 return nil
}
type echo struct { bus *inmem.Service }
func (e echo) Handle(ctx context.Context, event eventstream.Event) error {
 uid,ok:=eventstream.UserIDFromContext(ctx)
 if !ok {return errors.New("missing trusted identity")}
 return e.bus.Publish(ctx,uid,event)
}
func TestConsumer(t *testing.T) {
 bus := inmem.New()
 defer bus.Close()
 uid:=eventstream.NewUserID()
 upgrader,err:=websocketstream.NewUpgraderChecked([]string{"https://example.com"},nil,websocketstream.Config{})
 if err!=nil {t.Fatal(err)}
 h,err:=handlers.NewHTTPHandler(handlers.NewOptions(slog.Default(),bus,upgrader,nil,"",
  handlers.WithNetHTTPUserIDExtractor(func(r *http.Request)(eventstream.UserID,error){
   if r.Header.Get("Authorization")!="Bearer demo" {return eventstream.UserIDNil,errors.New("unauthorized")}
   return uid,nil
  }),
  handlers.WithEventAdapters(map[string]eventadapter.EventAdapter{"echo":eventadapter.NewEventProcessor[*message]()}),
  handlers.WithEventProcessors(map[string]eventprocessor.EventProcessor{"echo":echo{bus:bus}}),
 ))
 if err!=nil {t.Fatal(err)}
 defer h.Close()
 var handler http.Handler = h
 server:=httptest.NewServer(handler)
 defer server.Close()
 url:="ws"+strings.TrimPrefix(server.URL,"http")
 dialer:=libwebsocket.Dialer{HandshakeTimeout:time.Second}
 for _,tc:=range []struct{auth,origin string;status int}{
  {"","https://example.com",http.StatusUnauthorized},
  {"Bearer demo","https://invalid.example",http.StatusForbidden},
 } {
  conn,response,err:=dialer.Dial(url,http.Header{"Authorization":[]string{tc.auth},"Origin":[]string{tc.origin}})
  if conn!=nil {_=conn.Close()}
  if response!=nil {_=response.Body.Close()}
  if err==nil || response==nil || response.StatusCode!=tc.status {t.Fatalf("expected %d: %v %v",tc.status,response,err)}
 }
 conn,response,err:=dialer.Dial(url,http.Header{"Authorization":[]string{"Bearer demo"},"Origin":[]string{"https://example.com"}})
 if response!=nil {_=response.Body.Close()}
 if err!=nil {t.Fatal(err)}
 defer conn.Close()
 if err:=conn.SetReadDeadline(time.Now().Add(3*time.Second));err!=nil {t.Fatal(err)}
 if err:=conn.SetWriteDeadline(time.Now().Add(time.Second));err!=nil {t.Fatal(err)}
 input:=&message{ID:eventstream.NewEventID(),Type:"echo",Body:"external net/http consumer"}
 if err:=conn.WriteJSON(input);err!=nil {t.Fatal(err)}
 var output message
 if err:=conn.ReadJSON(&output);err!=nil {t.Fatal(err)}
 if output!=*input {t.Fatalf("round trip mismatch: %+v",output)}
 ctx,cancel:=context.WithTimeout(context.Background(),time.Second);defer cancel()
 if err:=h.Shutdown(ctx);err!=nil {t.Fatal(err)}
 if _,_,err:=conn.ReadMessage();!libwebsocket.IsCloseError(err,libwebsocket.CloseNormalClosure) {t.Fatalf("shutdown close: %v",err)}
 if stats:=h.Stats();stats.Active!=0 || stats.Accepted!=1 || stats.Rejected!=2 || stats.Incoming!=1 || stats.Outgoing!=1 {
  t.Fatalf("unexpected lifecycle stats: %+v",stats)
 }
}

GO
# By default only this checkout is replaced. Set GOWEBSOCKET_VERSION to check
# a published version or commit, without any replacement. Public dependencies
# and module checksums must resolve without a workspace or private access.
export GOWORK=off GOPRIVATE= GONOPROXY= GONOSUMDB= GOPROXY=https://proxy.golang.org GOSUMDB=sum.golang.org
go mod tidy
go test -race -timeout=1m ./...
