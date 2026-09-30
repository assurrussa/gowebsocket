#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
cd "$work"
cat >go.mod <<MOD
module consumer.example/gowebsocket-smoke

go 1.27.0

toolchain go1.27.1

require github.com/assurrussa/gowebsocket v0.0.0
replace github.com/assurrussa/gowebsocket => $root
MOD
cat >consumer_test.go <<'GO'
package consumer_test
import (
 "context"
 "testing"
 inmem "github.com/assurrussa/gowebsocket/eventstream/inmem"
 "github.com/assurrussa/gowebsocket/websocketstream"
)
func TestConsumer(t *testing.T) {
 bus := inmem.New()
 if err := bus.Shutdown(context.Background()); err != nil { t.Fatal(err) }
 if _, err := websocketstream.NewUpgraderChecked([]string{"https://example.com"}, nil, websocketstream.Config{}); err != nil { t.Fatal(err) }
}
GO
# Only the module under review is replaced; no sibling repositories, workspace,
# private proxy or credentials are needed for its public dependencies.
export GOWORK=off GOPRIVATE= GONOPROXY= GONOSUMDB=
go mod tidy
go test -race -timeout=1m ./...
