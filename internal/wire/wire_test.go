package wire_test

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"github.com/assurrussa/gowebsocket/internal/wire"
)

func TestFormatsAndLimits(t *testing.T) {
	payload := `{"eventType":"test"}`
	for _, format := range []wire.Format{wire.JSON, wire.LegacyBase64} {
		encoded := payload
		if format == wire.LegacyBase64 {
			encoded = base64.StdEncoding.EncodeToString([]byte(payload))
		}
		got, err := wire.Decode(strings.NewReader(encoded), format, 100, int64(len(payload)))
		if err != nil || string(got) != payload {
			t.Fatalf("round trip: %q %v", got, err)
		}
		if _, err := wire.Decode(strings.NewReader(encoded), format, 100, int64(len(payload)-1)); !errors.Is(err, wire.ErrTooLarge) {
			t.Fatalf("missing decoded limit: %v", err)
		}
		if _, err := wire.Decode(strings.NewReader(encoded), format, 1, 100); !errors.Is(err, wire.ErrTooLarge) {
			t.Fatalf("missing wire limit: %v", err)
		}
	}
	if _, err := wire.Decode(strings.NewReader("not base64!"), wire.LegacyBase64, 100, 100); !errors.Is(err, wire.ErrEncoding) {
		t.Fatal(err)
	}
	if _, err := wire.Decode(bytes.NewReader([]byte{255}), wire.JSON, 100, 100); !errors.Is(err, wire.ErrEncoding) {
		t.Fatal(err)
	}
}

func TestLimitAfterDecompression(t *testing.T) {
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write([]byte(strings.Repeat("x", 10000))); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := gzip.NewReader(&compressed)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := wire.Decode(reader, wire.JSON, 1000, 1000); !errors.Is(err, wire.ErrTooLarge) {
		t.Fatalf("decompressed body was not limited: %v", err)
	}
}

func FuzzDecode(f *testing.F) {
	f.Add([]byte(`{"eventType":"test"}`), false)
	f.Fuzz(func(t *testing.T, data []byte, legacy bool) {
		format := wire.JSON
		if legacy {
			format = wire.LegacyBase64
		}
		decoded, err := wire.Decode(bytes.NewReader(data), format, 1024, 512)
		if err == nil && len(decoded) > 512 {
			t.Fatal("decoded limit exceeded")
		}
	})
}
