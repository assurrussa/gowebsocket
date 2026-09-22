package websocketstream_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	websocketstream "github.com/assurrussa/gowebsocket/websocketstream"
)

func TestJSONEventWriter_Smoke(t *testing.T) {
	wr := websocketstream.JSONEventWriter{}
	out := bytes.NewBuffer(nil)
	err := wr.Write(struct{ Name string }{Name: "John"}, out)
	require.NoError(t, err)
	assert.JSONEq(t, `{"Name":"John"}`, out.String())
}

func TestJSONEventWriter_SmokeString(t *testing.T) {
	wr := websocketstream.JSONEventWriter{}
	out := bytes.NewBuffer(nil)
	err := wr.Write(`{"Name":"John"}`, out)
	require.NoError(t, err)
	assert.JSONEq(t, `{"Name":"John"}`, out.String())
}

func TestJSONEventWriter_SmokeBytes(t *testing.T) {
	wr := websocketstream.JSONEventWriter{}
	out := bytes.NewBuffer(nil)
	err := wr.Write([]byte(`{"Name":"John"}`), out)
	require.NoError(t, err)
	assert.JSONEq(t, `{"Name":"John"}`, out.String())
}
