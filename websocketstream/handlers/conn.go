package handlers

import libwebsocket "github.com/fasthttp/websocket"

// Conn preserves legacy metadata helpers. The handler no longer pools this
// wrapper or retains Fiber request metadata after upgrade. Metadata setters are
// single-owner operations, not concurrent mutable storage.
type Conn struct {
	*libwebsocket.Conn
	locals                   map[string]any
	params, cookies, queries map[string]string
	headers                  map[string][]string
	ip                       string
}

func (c *Conn) Locals(key string, value ...any) any {
	if len(value) == 0 {
		return c.locals[key]
	}
	if c.locals == nil {
		c.locals = make(map[string]any)
	}
	c.locals[key] = value[0]
	return value[0]
}

func metadata(values map[string]string, key string, fallback []string) string {
	value, ok := values[key]
	if !ok && len(fallback) > 0 {
		return fallback[0]
	}
	return value
}
func (c *Conn) Params(key string, fallback ...string) string {
	return metadata(c.params, key, fallback)
}
func (c *Conn) Query(key string, fallback ...string) string {
	return metadata(c.queries, key, fallback)
}
func (c *Conn) Cookies(key string, fallback ...string) string {
	return metadata(c.cookies, key, fallback)
}
func (c *Conn) Headers(key string, fallback ...string) []string {
	value, ok := c.headers[key]
	if !ok {
		value = fallback
	}
	return append([]string(nil), value...)
}
func (c *Conn) IP() string { return c.ip }
