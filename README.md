# kx

`kx` is kubectl with context algebra.

It is for Linux people with more than one cluster, too many namespaces, a
terminal full of muscle memory, and exactly zero patience for "wait, was that
prod?" moments.

No dashboard pilgrimage. No kubeconfig superstition. No bash loop graveyard.
`kubectl` stays the engine. `kx` becomes the operator shell around it.

```bash
kx get pods -A
kx @prod matrix -n payments
kx @prod matrix deploy/api -n payments --cols context,ready,image,rollout
kx @prod why deploy/api -n payments --deep
kx @prod logs deploy/api -n payments --since 15m --grep 'error|panic|timeout'
kx @prod diff deploy/api -n payments
```

## What kx Does

`kx` treats contexts like a queryable fleet:

- selectors: `@prod`, `@prod.eu`, `@env=prod`, `@team:payments`, `@/regex/`
- context tags that never touch kubeconfig
- guarded fan-out across many contexts
- incident commands: `why`, `events`, `logs`
- fleet views: `matrix`, `diff`, `drift`
- RBAC checks: `can`, `access`
- context hygiene: `ctx scan`, `ctx clean`
- shell glue: `completion`, `shell-init`, `prompt`
- Krew packaging template for release distribution

The mental model is:

```text
selector + command + policy + output
```

## Install

Latest Linux `amd64` release:

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
version="v0.3.0"
base="https://github.com/GabboPenna/kx/releases/download/$version"

curl -fsSLO "$base/kx-linux-amd64.tar.gz"
curl -fsSLO "$base/checksums.txt"
sha256sum -c checksums.txt --ignore-missing

tmp="$(mktemp -d)"
tar -xzf kx-linux-amd64.tar.gz -C "$tmp"
sudo install -m 0755 "$tmp/kx-linux-amd64/kx" /usr/local/bin/kx
```

From source:

```bash
go install github.com/GabboPenna/kx@latest
```

Local hacker loop:

```bash
git clone https://github.com/GabboPenna/kx
cd kx
make test
make build
sudo install -m 0755 bin/kx /usr/local/bin/kx
```

## Selectors

Selectors start with `@`.

| Selector | Meaning |
| --- | --- |
| `@all` | every kubeconfig context |
| `@current` | current kubectl context |
| `@prod` | fuzzy match context, cluster, user, namespace, and tags |
| `@prod.eu` | match all facets |
| `@env=prod` | match local kx tag |
| `@team:payments` | same as `@team=payments` |
| `@/aks-prod-.*/` | regex against context names |
| `@prod,@staging` | union of selectors |

Tag contexts once, then stop memorizing cloud-provider soup:

```bash
kx ctx tag aks-prod-weu env=prod region=eu team=payments risk=high
kx ctx tag aks-stage-weu env=staging region=eu team=payments
kx ctx where @prod.eu
```

## Fleet Commands

Normal kubectl still works:

```bash
kx get pods -A
kx apply -f app.yaml
```

Fan out when you add a selector:

```bash
kx @prod --parallel 8 get deploy -n payments
kx @prod --timeout 20s get nodes -o wide
kx @prod --canary 1 rollout restart deploy/api -n payments
```

`kx` never mutates the global current context while doing selector fan-out.

## Incident Mode

### why

`why` is the "tell me what hurts" command. It gathers the resource JSON,
kind-aware facts, conditions, owner chain, rollout status, warning events, and
optional deep describe/log detail.

```bash
kx why pod/api-6b7df8 -n payments
kx why svc/api -n payments
kx @prod why deploy/api -n payments
kx @prod why deploy/api -n payments --deep
```

### events

Events without the archaeological dig:

```bash
kx events -n payments --warnings --since 30m
kx @prod events -A --warnings --limit 80
```

### logs

Logs get context prefixes, grep, and all-container defaults:

```bash
kx logs deploy/api -n payments --since 10m
kx @prod logs deploy/api -n payments --since 15m --grep 'error|panic|timeout'
```

## Fleet Intelligence

### matrix

The matrix is the little ops table you wanted every time a namespace started
acting cursed at 02:00.

Resource mode compares one target across contexts:

```bash
kx @prod matrix svc -n payments
kx @prod matrix deploy/api -n payments
kx @prod matrix deploy/api -n payments --cols context,ready,image,replicas,rollout
```

Matrix output is kind-aware by default:

```text
svc       -> TYPE, CLUSTER-IP, EXTERNAL-IP, PORTS, ENDPOINTS, SELECTOR
pod       -> STATUS, READY, RESTARTS, POD-IP, NODE, IMAGE
deploy    -> STATUS, READY, REPLICAS, IMAGE, ROLLOUT
ingress   -> CLASS, HOSTS, ADDRESS, PORTS, STATUS
job       -> STATUS, COMPLETIONS, SUCCEEDED, FAILED
cronjob   -> STATUS, SCHEDULE, SUSPEND, ACTIVE, LAST-SCHEDULE
pvc/pv    -> STATUS, VOLUME, CAPACITY, ACCESS-MODES, STORAGECLASS
node      -> STATUS, ROLES, INTERNAL-IP, VERSION
```

Namespace mode scans a whole namespace and prints a compact inventory using
generic kind-aware columns: `STATUS`, `DETAIL`, `NETWORK`, `ENDPOINTS`, and
`AGE`.

```bash
kx matrix -n payments
kx @prod matrix -n payments
kx @prod matrix -n payments --resources deployments,pods,services,ingresses
kx @prod matrix -n payments --cols context,kind,name,status,ready,age
```

By default namespace mode checks:

```text
deployments,statefulsets,daemonsets,pods,services,ingresses,jobs,cronjobs
```

Output is intentionally boring in the best possible way: fixed table headers,
bounded columns, deterministic sorting, and enough status translation to spot
`ImagePullBackOff`, degraded workloads, suspended CronJobs, pending Ingresses,
pending LoadBalancers, and Services with zero ready endpoints without cracking
open another pane.

### diff

Sanitized resource diff. It strips runtime noise like `status`, `uid`,
`resourceVersion`, `managedFields`, and timestamps before comparing.

```bash
kx @staging,@prod diff deploy/api -n payments
```

### drift

When you only need the shape of the problem:

```bash
kx @prod drift deploy/api -n payments
```

## RBAC Without Yoga

Check one permission across the fleet:

```bash
kx @prod can delete pods -n kube-system
kx @prod can create deployments -n payments
```

Print a quick access matrix:

```bash
kx access -n payments
kx @prod access -n payments --resources pods,secrets,deployments --verbs get,list,create,delete
```

## Context Hygiene

Find kubeconfig fragments:

```bash
kx ctx scan ~/.kube ~/.kube/configs.d
```

Clean local `kx` metadata for contexts that no longer exist in your active
kubeconfig:

```bash
kx ctx clean --dry-run
kx ctx clean
```

`kx` stores its own metadata under:

```text
~/.config/kx
```

Override it when you want disposable labs:

```bash
export KX_HOME="$PWD/.kx"
```

## Shell Glue

Completion:

```bash
kx completion bash > ~/.local/share/bash-completion/completions/kx
kx completion zsh > ~/.zfunc/_kx
kx completion fish > ~/.config/fish/completions/kx.fish
```

Prompt segment:

```bash
kx prompt
```

Full shell init:

```bash
eval "$(kx shell-init bash)"
```

That gives you command completion plus a tiny context segment in your prompt.
The point is constant context awareness without a spaceship prompt framework.

## Safety

Mutating commands such as `apply`, `delete`, `patch`, `scale`, `rollout restart`,
`drain`, `cordon`, `label`, and `annotate` trigger confirmation when they target
multiple contexts or prod-like contexts.

A context is prod-like when:

- its name contains `prod`
- the selector contains `prod`
- it has `env=prod`
- it has `risk=high`

Automation can opt in explicitly:

```bash
kx @prod --yes rollout restart deploy/api -n payments
KX_FORCE=1 kx @prod rollout restart deploy/api -n payments
```

## Krew

Release assets are shaped for Krew. Generate a manifest after a release:

```bash
version="v0.2.0"
curl -fsSLO "https://github.com/GabboPenna/kx/releases/download/$version/checksums.txt"
scripts/render-krew-manifest.sh "$version" checksums.txt > packaging/krew/kx.yaml
```

The template lives at:

```text
packaging/krew/kx.template.yaml
```

## Releases

`kx` uses SemVer tags:

```text
v0.1.0
v0.2.0
v1.0.0
```

Pushing a tag creates a GitHub Release with:

```text
kx-linux-amd64.tar.gz
kx-linux-arm64.tar.gz
checksums.txt
kx-krew.yaml
```

The binary embeds tag, commit, and build timestamp:

```bash
kx --version
```

See `docs/RELEASING.md` for the release flow.

## Design

`kx` is written in Go because this job wants:

- one boring binary
- fast startup
- cheap fan-out concurrency
- Linux-first release assets
- easy process control around `kubectl`
- no dependency carnival during an incident

The first rule is compatibility: unknown Kubernetes behavior goes to `kubectl`.
The second rule is context sanity: fleet operations should be explicit,
queryable, repeatable, and hard to fat-finger.

## License

MIT
