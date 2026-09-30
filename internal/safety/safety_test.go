package safety_test

import (
	"errors"
	"testing"

	"github.com/assurrussa/gowebsocket/internal/safety"
)

func TestPanicAndTypedNil(t *testing.T) {
	err := safety.Call(func() error { panic("secret") })
	if !errors.Is(err, safety.ErrPanic) || err.Error() != "application callback panicked" {
		t.Fatal(err)
	}
	var pointer *int
	var value any = pointer
	if !safety.IsNil(value) || safety.IsNil(1) {
		t.Fatal("typed nil check")
	}
}
