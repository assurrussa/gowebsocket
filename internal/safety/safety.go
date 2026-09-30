// Package safety isolates panics in application-provided callbacks.
package safety

import (
	"errors"
	"reflect"
)

// ErrPanic deliberately excludes the panic value: it may contain user data.
var ErrPanic = errors.New("application callback panicked")

// Call converts a callback panic into a safe error. It cannot interrupt a
// blocking callback; callbacks must honor their context and return promptly.
func Call(fn func() error) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrPanic
		}
	}()
	return fn()
}

// IsNil also recognizes an interface containing a typed nil.
func IsNil(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
