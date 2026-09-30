// Package wire decodes bounded WebSocket payloads independently of the transport.
package wire

import (
	"bytes"
	"encoding/base64"
	"errors"
	"io"
	"unicode/utf8"
)

type Format string

const (
	LegacyBase64 Format = "base64-json"
	JSON         Format = "json"
)

var (
	ErrTooLarge = errors.New("message exceeds configured limit")
	ErrEncoding = errors.New("invalid message encoding")
)

// Decode limits both the post-decompression WebSocket message and the decoded
// application payload. The underlying WebSocket read limit is a separate guard.
func Decode(reader io.Reader, format Format, maxWire, maxDecoded int64) ([]byte, error) {
	if maxWire <= 0 || maxDecoded <= 0 || maxWire == int64(^uint64(0)>>1) || maxDecoded == int64(^uint64(0)>>1) {
		return nil, errors.New("invalid message limits")
	}
	raw, err := io.ReadAll(io.LimitReader(reader, maxWire+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > maxWire {
		return nil, ErrTooLarge
	}
	if !utf8.Valid(raw) {
		return nil, ErrEncoding
	}
	switch format {
	case JSON:
		if int64(len(raw)) > maxDecoded {
			return nil, ErrTooLarge
		}
		return raw, nil
	case LegacyBase64:
		decoded, err := io.ReadAll(io.LimitReader(base64.NewDecoder(base64.StdEncoding, bytes.NewReader(raw)), maxDecoded+1))
		if err != nil {
			return nil, ErrEncoding
		}
		if int64(len(decoded)) > maxDecoded {
			return nil, ErrTooLarge
		}
		if !utf8.Valid(decoded) {
			return nil, ErrEncoding
		}
		return decoded, nil
	default:
		return nil, ErrEncoding
	}
}
