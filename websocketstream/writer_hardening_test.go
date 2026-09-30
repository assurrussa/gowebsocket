package websocketstream_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/assurrussa/gowebsocket/websocketstream"
)

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }

func TestWriterValidationAndShortWrite(t *testing.T) {
	writer := websocketstream.JSONEventWriter{}
	if err := writer.Write("not JSON", io.Discard); err == nil {
		t.Fatal("invalid raw JSON accepted")
	}
	if err := writer.Write(json.RawMessage(`{"valid":true}`), shortWriter{}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := (websocketstream.StrictJSONEventWriter{}).Write("text", &output); err != nil {
		t.Fatal(err)
	}
	if output.String() != "\"text\"\n" {
		t.Fatal(output.String())
	}
}
