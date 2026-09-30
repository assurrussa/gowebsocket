package handlers

import (
	"fmt"
	"reflect"

	"github.com/google/uuid"

	"github.com/assurrussa/gowebsocket/eventstream"
	"github.com/assurrussa/gowebsocket/internal/safety"
)

type userWithEventstreamUUID interface{ GetUUID() eventstream.UserID }
type userWithUUIDUUID interface{ GetUUID() uuid.UUID }
type userWithStringUUID interface{ GetUUID() string }

func toUserID(value any) (eventstream.UserID, bool) {
	if safety.IsNil(value) {
		return eventstream.UserIDNil, false
	}
	var id eventstream.UserID
	switch value := value.(type) {
	case eventstream.UserID:
		id = value
	case uuid.UUID:
		id = eventstream.UserID(value)
	case [16]byte:
		id = eventstream.UserID(value)
	case string:
		parsed, err := eventstream.ParseUserID(value)
		if err != nil {
			return eventstream.UserIDNil, false
		}
		id = parsed
	case fmt.Stringer:
		parsed, err := eventstream.ParseUserID(value.String())
		if err != nil {
			return eventstream.UserIDNil, false
		}
		id = parsed
	default:
		return eventstream.UserIDNil, false
	}
	return id, !id.IsZero()
}

// getUserID is the legacy compatibility path. New applications should use an
// explicit UserIDExtractor. A present but invalid GetUUID never falls back to
// Stringer, which may represent a session ID rather than a user ID.
func (h *HTTPHandler) getUserID(conn *Conn) (eventstream.UserID, bool) {
	value := conn.Locals(h.userIDCtxKey)
	if safety.IsNil(value) {
		return eventstream.UserIDNil, false
	}
	switch user := value.(type) {
	case eventstream.UserID, uuid.UUID, [16]byte:
		return toUserID(user)
	case userWithEventstreamUUID:
		return toUserID(user.GetUUID())
	case userWithUUIDUUID:
		return toUserID(user.GetUUID())
	case userWithStringUUID:
		return toUserID(user.GetUUID())
	}
	if id, valid, present := getUserIDByReflection(value); present {
		return id, valid
	}
	return toUserID(value)
}

func getUserIDByReflection(value any) (eventstream.UserID, bool, bool) {
	if safety.IsNil(value) {
		return eventstream.UserIDNil, false, false
	}
	rv := reflect.ValueOf(value)
	method := rv.MethodByName("GetUUID")
	if !method.IsValid() && rv.Kind() == reflect.Struct {
		pointer := reflect.New(rv.Type())
		pointer.Elem().Set(rv)
		method = pointer.MethodByName("GetUUID")
	}
	if !method.IsValid() {
		return eventstream.UserIDNil, false, false
	}
	if method.Type().NumIn() != 0 || method.Type().NumOut() != 1 {
		return eventstream.UserIDNil, false, true
	}
	result := method.Call(nil)
	id, ok := toUserID(result[0].Interface())
	return id, ok, true
}
