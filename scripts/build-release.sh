#!/usr/bin/env sh
set -eu

VERSION="${VERSION:-$(git describe --tags --always --dirty 2>/dev/null || echo dev)}"
COMMIT="${COMMIT:-$(git rev-parse --short HEAD 2>/dev/null || echo unknown)}"
DATE="${DATE:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
VERSION_PKG="github.com/GabboPenna/kx/internal/cli"
LDFLAGS="-s -w -X ${VERSION_PKG}.version=${VERSION} -X ${VERSION_PKG}.commit=${COMMIT} -X ${VERSION_PKG}.date=${DATE}"

rm -rf dist
mkdir -p dist

build_linux() {
  arch="$1"
  workdir="dist/kx-linux-${arch}"

  mkdir -p "$workdir"
  GOOS=linux GOARCH="$arch" CGO_ENABLED=0 \
    go build -trimpath -buildvcs=false -ldflags "$LDFLAGS" -o "$workdir/kx" .

  cp README.md LICENSE "$workdir/"
  tar -C dist -czf "dist/kx-linux-${arch}.tar.gz" "kx-linux-${arch}"
  rm -rf "$workdir"
}

build_linux amd64
build_linux arm64

(
  cd dist
  sha256sum kx-linux-*.tar.gz > checksums.txt
)
