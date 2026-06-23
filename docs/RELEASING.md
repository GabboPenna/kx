# Releasing kx

`kx` uses SemVer tags and GitHub Release assets.

The tag is the source of truth. The repository never stores built binaries.
Release binaries are created by GitHub Actions on Ubuntu and attached to the
matching GitHub Release.

## Versioning

Use SemVer with a leading `v`:

```text
v0.1.0
v0.2.0
v1.0.0
```

Pre-releases are allowed:

```text
v0.2.0-rc.1
v0.2.0-beta.1
```

Rules:

- patch: bug fixes, small behavior fixes, docs that ship with the binary
- minor: new commands, new selectors, new output modes, compatible behavior
- major: breaking CLI contracts or incompatible storage/history changes

## Release assets

Each release publishes:

```text
kx-linux-amd64.tar.gz
kx-linux-arm64.tar.gz
checksums.txt
kx-krew.yaml
```

Each tarball contains:

```text
kx
README.md
LICENSE
```

## Release flow

From a clean `main` branch:

```bash
git switch main
git pull --ff-only
make test
make build
git tag -a v0.1.0 -m "kx v0.1.0"
git push origin main
git push origin v0.1.0
```

The tag push triggers `.github/workflows/release.yml`.

## Local release dry run

Build the same Linux assets locally:

```bash
VERSION=v0.1.0 sh scripts/build-release.sh
scripts/render-krew-manifest.sh v0.1.0 dist/checksums.txt > dist/kx-krew.yaml
ls -lh dist/
sha256sum -c dist/checksums.txt
```

## Installing a release

Latest `amd64` build:

```bash
tmp="$(mktemp -d)"
curl -fsSL -o "$tmp/kx.tar.gz" \
  https://github.com/GabboPenna/kx/releases/latest/download/kx-linux-amd64.tar.gz
tar -xzf "$tmp/kx.tar.gz" -C "$tmp"
sudo install -m 0755 "$tmp/kx-linux-amd64/kx" /usr/local/bin/kx
kx --version
```

Verify checksum for a pinned tag:

```bash
version="v0.1.0"
base="https://github.com/GabboPenna/kx/releases/download/$version"
curl -fsSLO "$base/kx-linux-amd64.tar.gz"
curl -fsSLO "$base/checksums.txt"
sha256sum -c checksums.txt --ignore-missing
```
