package eventstream_test

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/assurrussa/gowebsocket/eventstream"
)

func TestEventID(t *testing.T) {
	t.Parallel()

	t.Run("zero value and new", func(t *testing.T) {
		t.Parallel()

		var zero eventstream.EventID
		assert.True(t, zero.IsZero())
		assert.Equal(t, eventstream.EventIDNil, zero)
		assert.Nil(t, zero.AsPointer())
		require.Error(t, zero.Validate())

		eid := eventstream.NewEventID()
		assert.False(t, eid.IsZero())
		assert.NotEqual(t, eventstream.EventIDNil, eid)
		assert.NotEmpty(t, eid.String())
		require.NoError(t, eid.Validate())
		assert.NotNil(t, eid.AsPointer())
		assert.Equal(t, eid, *eid.AsPointer())
	})

	t.Run("matches", func(t *testing.T) {
		t.Parallel()

		eid := eventstream.NewEventID()
		assert.True(t, eid.Matches(eid))
		assert.False(t, eid.Matches(eventstream.NewEventID()))
		assert.False(t, eid.Matches("not-event-id"))
	})

	t.Run("sql driver value and scan", func(t *testing.T) {
		t.Parallel()

		eid := eventstream.NewEventID()
		val, err := eid.Value()
		require.NoError(t, err)
		assert.Equal(t, eid.String(), val)

		var scanned eventstream.EventID
		require.NoError(t, scanned.Scan(eid.String()))
		assert.Equal(t, eid, scanned)

		require.NoError(t, scanned.Scan([]byte(eid.String())))
		assert.Equal(t, eid, scanned)
	})

	t.Run("json marshal and unmarshal", func(t *testing.T) {
		t.Parallel()

		type payload struct {
			ID eventstream.EventID `json:"id"`
		}

		eid := eventstream.NewEventID()
		data, err := json.Marshal(payload{ID: eid})
		require.NoError(t, err)

		var decoded payload
		require.NoError(t, json.Unmarshal(data, &decoded))
		assert.Equal(t, eid, decoded.ID)
	})

	t.Run("parse and must parse", func(t *testing.T) {
		t.Parallel()

		raw := uuid.New().String()
		eid, err := eventstream.ParseEventID(raw)
		require.NoError(t, err)
		assert.Equal(t, raw, eid.String())

		assert.Equal(t, eid, eventstream.MustParseEventID(raw))
		assert.Equal(t, eid, eventstream.MustParse[eventstream.EventID](raw))

		_, err = eventstream.ParseEventID("invalid")
		assert.Error(t, err)
	})
}

func TestUserID(t *testing.T) {
	t.Parallel()

	t.Run("zero value and new", func(t *testing.T) {
		t.Parallel()

		var zero eventstream.UserID
		assert.True(t, zero.IsZero())
		assert.Equal(t, eventstream.UserIDNil, zero)
		assert.Nil(t, zero.AsPointer())
		require.Error(t, zero.Validate())

		uid := eventstream.NewUserID()
		assert.False(t, uid.IsZero())
		assert.NotEqual(t, eventstream.UserIDNil, uid)
		assert.NotEmpty(t, uid.String())
		require.NoError(t, uid.Validate())
		assert.NotNil(t, uid.AsPointer())
		assert.Equal(t, uid, *uid.AsPointer())
	})

	t.Run("matches", func(t *testing.T) {
		t.Parallel()

		uid := eventstream.NewUserID()
		assert.True(t, uid.Matches(uid))
		assert.False(t, uid.Matches(eventstream.NewUserID()))
		assert.False(t, uid.Matches(123))
	})

	t.Run("sql driver value and scan", func(t *testing.T) {
		t.Parallel()

		uid := eventstream.NewUserID()
		val, err := uid.Value()
		require.NoError(t, err)
		assert.Equal(t, uid.String(), val)

		var scanned eventstream.UserID
		require.NoError(t, scanned.Scan(uid.String()))
		assert.Equal(t, uid, scanned)

		require.NoError(t, scanned.Scan([]byte(uid.String())))
		assert.Equal(t, uid, scanned)
	})

	t.Run("json marshal and unmarshal", func(t *testing.T) {
		t.Parallel()

		type payload struct {
			ID eventstream.UserID `json:"id"`
		}

		uid := eventstream.NewUserID()
		data, err := json.Marshal(payload{ID: uid})
		require.NoError(t, err)

		var decoded payload
		require.NoError(t, json.Unmarshal(data, &decoded))
		assert.Equal(t, uid, decoded.ID)
	})

	t.Run("parse and must parse", func(t *testing.T) {
		t.Parallel()

		raw := uuid.New().String()
		uid, err := eventstream.ParseUserID(raw)
		require.NoError(t, err)
		assert.Equal(t, raw, uid.String())

		assert.Equal(t, uid, eventstream.MustParseUserID(raw))
		assert.Equal(t, uid, eventstream.MustParse[eventstream.UserID](raw))

		_, err = eventstream.ParseUserID("invalid")
		assert.Error(t, err)
	})
}
