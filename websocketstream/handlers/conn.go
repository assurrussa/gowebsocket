package handlers

import (
	"sync"

	libwebsocket "github.com/fasthttp/websocket"
)

// Conn https://godoc.org/github.com/fasthttp/websocket#pkg-index
type Conn struct {
	*libwebsocket.Conn
	locals  map[string]any
	params  map[string]string
	cookies map[string]string
	headers map[string][]string
	queries map[string]string
	ip      string
}

// Conn pool.
var poolConn = sync.Pool{
	New: func() any {
		return new(Conn)
	},
}

// Acquire Conn from pool.
func acquireConn() *Conn {
	v := poolConn.Get()
	conn, ok := v.(*Conn)
	if !ok || conn == nil {
		conn = &Conn{}
	}
	conn.locals = make(map[string]any)
	conn.params = make(map[string]string)
	conn.queries = make(map[string]string)
	conn.cookies = make(map[string]string)
	conn.headers = make(map[string][]string)
	return conn
}

// Return Conn to pool.
func releaseConn(conn *Conn) {
	conn.Conn = nil
	poolConn.Put(conn)
}

// Locals makes it possible to pass any values under string keys scoped to the request
// and therefore available to all following routes that match the request.
func (conn *Conn) Locals(key string, value ...any) any {
	if len(value) == 0 {
		return conn.locals[key]
	}
	conn.locals[key] = value[0]
	return value[0]
}

// Params is used to get the route parameters.
// Defaults to empty string "" if the param doesn't exist.
// If a default value is given, it will return that value if the param doesn't exist.
func (conn *Conn) Params(key string, defaultValue ...string) string {
	v, ok := conn.params[key]
	if !ok && len(defaultValue) > 0 {
		return defaultValue[0]
	}
	return v
}

// Query returns the query string parameter in the url.
// Defaults to empty string "" if the query doesn't exist.
// If a default value is given, it will return that value if the query doesn't exist.
func (conn *Conn) Query(key string, defaultValue ...string) string {
	v, ok := conn.queries[key]
	if !ok && len(defaultValue) > 0 {
		return defaultValue[0]
	}
	return v
}

// Cookies is used for getting a cookie value by key
// Defaults to empty string "" if the cookie doesn't exist.
// If a default value is given, it will return that value if the cookie doesn't exist.
func (conn *Conn) Cookies(key string, defaultValue ...string) string {
	v, ok := conn.cookies[key]
	if !ok && len(defaultValue) > 0 {
		return defaultValue[0]
	}
	return v
}

// Headers is used for getting a header value by key
// Defaults to empty string "" if the header doesn't exist.
// If a default value is given, it will return that value if the header doesn't exist.
func (conn *Conn) Headers(key string, defaultValue ...string) []string {
	v, ok := conn.headers[key]
	if !ok && len(defaultValue) > 0 {
		return defaultValue
	}
	return v
}

// IP returns the client's network address.
func (conn *Conn) IP() string {
	return conn.ip
}
