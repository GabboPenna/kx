#!/usr/bin/env sh
set -eu

if [ "$#" -ne 2 ]; then
  echo "usage: scripts/render-krew-manifest.sh <version> <checksums.txt>" >&2
  exit 2
fi

version="$1"
checksums="$2"
template="packaging/krew/kx.template.yaml"

amd64="$(awk '/kx-linux-amd64\.tar\.gz/ {print $1}' "$checksums")"
arm64="$(awk '/kx-linux-arm64\.tar\.gz/ {print $1}' "$checksums")"

if [ -z "$amd64" ] || [ -z "$arm64" ]; then
  echo "checksums.txt must contain kx-linux-amd64.tar.gz and kx-linux-arm64.tar.gz" >&2
  exit 1
fi

sed \
  -e "s/{{VERSION}}/$version/g" \
  -e "s/{{SHA256_LINUX_AMD64}}/$amd64/g" \
  -e "s/{{SHA256_LINUX_ARM64}}/$arm64/g" \
  "$template"
