GO ?= go
GOFMT ?= gofmt
GOFLAGS ?= -trimpath -buildvcs=false
BINARY ?= kx
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
VERSION_PKG := github.com/GabboPenna/kx/internal/cli
LDFLAGS ?= -s -w -X $(VERSION_PKG).version=$(VERSION) -X $(VERSION_PKG).commit=$(COMMIT) -X $(VERSION_PKG).date=$(DATE)

.PHONY: all build test fmt clean

all: test build

fmt:
	$(GOFMT) -w $$(find . -path './.tools' -prune -o -name '*.go' -print)

test:
	$(GO) test ./...

build:
	mkdir -p bin
	$(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o bin/$(BINARY) .

clean:
	rm -rf bin dist coverage.out
