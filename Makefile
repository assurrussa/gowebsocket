.DEFAULT_GOAL := check
GOLANGCI_VERSION := v2.14.0
export GOFLAGS ?= -mod=readonly
GO_FILES := $(shell find . -type f -name '*.go' -not -path './vendor/*')

.PHONY: check fmt-check vet lint test test-race examples consumer fix fmt tidy generate bench-all cover-html

check: fmt-check vet lint test test-race examples consumer

fmt-check:
	@test -z "$$(gofmt -l $(GO_FILES))" || (gofmt -l $(GO_FILES); exit 1)

vet:
	go vet ./...

lint:
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_VERSION) run --timeout=5m ./...

test:
	go test -timeout=3m ./...

test-race:
	go test -race -count=5 -timeout=5m ./...

examples:
	@tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT; go build -o "$$tmp/" ./examples/...

consumer:
	bash scripts/test-consumer.sh

fix:
	gofmt -w $(GO_FILES)
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_VERSION) fmt

fmt: fix

tidy:
	go mod tidy

generate:
	go generate ./...

bench-all:
	go test -run='^$$' -bench=. -benchmem ./...

cover-html:
	go test -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -html=coverage.out -o cover.html
