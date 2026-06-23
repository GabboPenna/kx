# kx

`kx` is kubectl with context algebra.

The idea is simple: I do not want another dashboard, and I do not want to keep
teaching my shell which cluster I meant. I want one tiny binary that behaves like
`kubectl` until I ask it to think in fleets.

```bash
kx get pods -A
kx @prod --parallel 4 get deploy -n payments
kx @env=prod --dry-run delete pod old-api -n payments
kx ctx tag aks-prod-weu env=prod region=eu team=payments risk=high
```

## Why this exists

Kubernetes contexts are powerful, but daily multi-cluster work still feels like
taping together shell loops, prompt hacks, fuzzy switchers, and careful prayers.

`kx` keeps kubectl as the transport layer and adds the missing operator layer:

- context selectors like `@prod`, `@prod.eu`, `@env=prod`, `@/regex/`
- local context tags that do not touch kubeconfig
- safe fan-out across many contexts
- deterministic grouped output
- JSONL output for `jq`, `rg`, `awk`, logs, and other terminal creature comforts
- command history for multi-context runs
- production-aware confirmation prompts
- zero global context mutation when using selectors

## Status

This is the first cut. It is intentionally small, sharp, and boring where boring
matters. Unknown Kubernetes behavior is delegated to `kubectl`; `kx` owns context
selection, fan-out, safety, and output.

## Kubernetes compatibility

`kx` shells out to `kubectl`, so it follows the kubectl you have installed.

The Kubernetes project publishes the latest stable kubectl download through:

```bash
https://dl.k8s.io/release/stable.txt
```

That means `kx` should not bake in a Kubernetes minor version. If kubectl can run
the command, `kx` can route it.

## Platform stance

`kx` is a Linux-first shell tool.

It is written in Go and should build anywhere Go and `kubectl` run, but the
project voice, examples, defaults, and development workflow assume a Linux
terminal. The happy path is `bash`, `make`, `jq`, `rg`, kubeconfig, and a real
operator shell.

## Install from source

```bash
go install github.com/GabboPenna/kx@latest
```

For local development:

```bash
git clone https://github.com/GabboPenna/kx
cd kx
make test
make build
sudo install -m 0755 bin/kx /usr/local/bin/kx
```

## Install from a release

Latest Linux `amd64` build:

```bash
tmp="$(mktemp -d)"
curl -fsSL -o "$tmp/kx.tar.gz" \
  https://github.com/GabboPenna/kx/releases/latest/download/kx-linux-amd64.tar.gz
tar -xzf "$tmp/kx.tar.gz" -C "$tmp"
sudo install -m 0755 "$tmp/kx-linux-amd64/kx" /usr/local/bin/kx
kx --version
```

Pinned release with checksum verification:

```bash
version="v0.1.0"
base="https://github.com/GabboPenna/kx/releases/download/$version"

curl -fsSLO "$base/kx-linux-amd64.tar.gz"
curl -fsSLO "$base/checksums.txt"
sha256sum -c checksums.txt --ignore-missing

tmp="$(mktemp -d)"
tar -xzf kx-linux-amd64.tar.gz -C "$tmp"
sudo install -m 0755 "$tmp/kx-linux-amd64/kx" /usr/local/bin/kx
```

## Quick start

List contexts:

```bash
kx ctx ls
```

Tag a context:

```bash
kx ctx tag aks-prod-weu env=prod region=eu team=payments risk=high
```

Preview a selector:

```bash
kx ctx where @prod.eu
```

Run a normal kubectl command:

```bash
kx get pods -A
```

Run the same command across selected contexts:

```bash
kx @team=payments --parallel 4 get deploy -n payments
```

Plan without touching clusters:

```bash
kx @prod --dry-run rollout restart deploy/api -n payments
```

Machine output:

```bash
kx @prod --jsonl get pods -A | jq -r 'select(.line | test("CrashLoopBackOff"))'
```

## Selectors

Selectors start with `@`.

| Selector | Meaning |
| --- | --- |
| `@all` | every kubeconfig context |
| `@current` | current kubectl context |
| `@prod` | fuzzy match against context names, cluster names, users, namespaces, and tags |
| `@prod.eu` | match all facets |
| `@env=prod` | match local kx tag |
| `@team:payments` | same as `@team=payments` |
| `@/aks-prod-.*/` | regex against context names |
| `@prod,@staging` | union of selectors |

The `@prod.eu` form is the sweet spot. I want to type the way I think:
"production, Europe, whatever the real context name is today".

## Safety model

`kx` is fast, but it should not be brave on your behalf.

Mutating commands such as `apply`, `delete`, `patch`, `scale`, `rollout restart`,
`drain`, `cordon`, `label`, and `annotate` trigger confirmation when they target
multiple contexts or prod-like contexts.

A context is prod-like when:

- its name contains `prod`
- its `env` tag is `prod`
- its `risk` tag is `high`
- the selector contains `prod`

Automation can use:

```bash
kx @prod --yes rollout restart deploy/api -n payments
```

or:

```bash
KX_FORCE=1 kx @prod rollout restart deploy/api -n payments
```

## Options

`kx` options go before the kubectl command:

```bash
kx @prod --parallel 8 --timeout 20s get pods -A
```

| Option | Description |
| --- | --- |
| `--parallel N` | run N contexts at a time |
| `--timeout 20s` | stop slow context calls |
| `--fail-fast` | skip pending work after the first failure |
| `--dry-run` | print the selected contexts and command only |
| `--canary N` | run only the first N matched contexts |
| `--jsonl` | print line-oriented JSON |
| `--no-header` | suppress grouped context headers |
| `--yes`, `-y` | approve safety prompts |

## Config files

`kx` never writes to kubeconfig for tags or history.

On Linux, `kx` stores metadata under:

```text
~/.config/kx
```

Override it with:

```bash
export KX_HOME="$HOME/.kx"
```

Stored files:

```text
contexts.json
history.jsonl
```

## Design notes

`kx` is written in Go because the shape of the problem wants:

- one static-feeling binary
- fast startup
- boring Linux builds
- cheap concurrency
- easy process control around `kubectl`
- boring deployment

The first version intentionally uses only the Go standard library. Fewer moving
parts means fewer reasons for the tool to be annoying when you are already deep
inside an incident.

## Releases

`kx` uses SemVer tags:

```text
v0.1.0
v0.2.0
v1.0.0
```

Pushing a tag like `v0.1.0` creates a GitHub Release with:

```text
kx-linux-amd64.tar.gz
kx-linux-arm64.tar.gz
checksums.txt
```

Release notes are generated from the tagged commit history. The binary embeds
the tag, commit, and build timestamp, so `kx --version` tells you exactly what
you are running.

See `docs/RELEASING.md` for the full release flow.

## Roadmap

- `kx history rerun failed`
- `kx diff @prod get deploy/api -n payments`
- shell completions for selectors and tags
- Krew manifest
- context groups committed to a repo
- OpenTelemetry spans for fan-out runs
- policy packs for dangerous verbs
- plugin hooks before and after fan-out

## License

MIT
