package cli

import (
	"fmt"
	"io"
	"strings"
)

var topicHelp = map[string]string{
	"selectors": `Selectors choose kubeconfig contexts without changing your current context.

  @all                 every context
  @current             the current context
  @prod                fuzzy match names, cluster, user, namespace, or tags
  @prod.eu             require every facet
  @env=prod            exact local tag match
  @/aks-prod-.*/       regular expression against context names
  @prod,@staging       union, preserving kubeconfig order

Preview first:
  kx ctx where @prod
  kx @prod --dry-run get pods -A
`,
	"ctx": `Usage:
  kx ctx ls
  kx ctx show <context>
  kx ctx where <selector>
  kx ctx tag <context> key=value [key=value...]
  kx ctx untag <context> key [key...]
  kx ctx scan [path...]
  kx ctx clean [--dry-run]

Tags are local to kx; kubeconfig is never modified. Recommended tags are
env, region, team, and risk. The values env=prod and risk=high activate the
production safety guard independently of context naming.
`,
	"why": `Usage: kx [@selector] why <resource> [-n namespace] [--deep]

Summarizes status, readiness, conditions, owners, rollout, and warning events.
--deep also includes bounded describe output and pod logs.

  kx why pod/api-6b7df8 -n payments
  kx @prod why deploy/api -n payments --deep
`,
	"matrix": `Usage: kx [@selector] matrix [resource] [kubectl flags]
       kx [@selector] matrix -n <namespace> [--resources <csv>] [--cols <csv>]

Resource mode compares one target. Namespace mode builds a compact inventory.
Columns are kind-aware by default.

  kx @prod matrix deploy/api -n payments
  kx @prod matrix -n payments --resources deployments,pods,services
`,
	"diff": `Usage: kx [@selector] diff <resource> [-n namespace]

Compares resources after removing runtime fields such as status, UID,
resourceVersion, managedFields, and timestamps.
`,
	"drift": `Usage: kx [@selector] drift <resource> [-n namespace]

Groups contexts by the hash of their sanitized resource definition.
`,
	"logs": `Usage: kx [@selector] logs <resource> [kubectl log flags] [--grep <regex>]

Output is prefixed with context and stream. All containers are selected unless
you explicitly choose one.

  kx @prod logs deploy/api -n payments --since 15m --grep 'error|panic'
`,
	"events": `Usage: kx [@selector] events [-n namespace|-A] [--warnings] [--since 30m] [--limit 80]

Prints recent events in chronological order with bounded messages.
`,
	"can": `Usage: kx [@selector] can <verb> <resource> [-n namespace]

  kx @prod can delete pods -n payments
`,
	"access": `Usage: kx [@selector] access [-n namespace] [--resources <csv>] [--verbs <csv>]

Prints a compact RBAC permission matrix across contexts.
`,
	"history": `Usage: kx history

Shows the 20 most recent selector-based runs and whether they succeeded.
`,
	"doctor": `Usage: kx doctor

Checks kubectl discovery, kubeconfig parsing, current context, kx storage, and
tag coverage. It does not contact the Kubernetes API server.
`,
	"completion": `Usage: kx completion <bash|zsh|fish>
       kx shell-init <bash|zsh|fish>

  kx completion bash > ~/.local/share/bash-completion/completions/kx
  eval "$(kx shell-init bash)"
`,
}

func runHelpCommand(args []string, stdout io.Writer, stderr io.Writer) int {
	if len(args) == 0 {
		printHelp(stdout)
		return 0
	}
	topic := strings.ToLower(args[0])
	if topic == "context" || topic == "contexts" {
		topic = "ctx"
	}
	if _, ok := topicHelp[topic]; !ok {
		fmt.Fprintf(stderr, "kx: unknown help topic %q\n", args[0])
		fmt.Fprintln(stderr, "try: kx help selectors, kx help ctx, or kx help matrix")
		return 2
	}
	printTopicHelp(stdout, topic)
	return 0
}

func printTopicHelp(w io.Writer, topic string) {
	fmt.Fprintf(w, "kx %s\n\n%s", topic, topicHelp[topic])
}

func wantsHelp(args []string) bool {
	return len(args) > 0 && (args[0] == "help" || args[0] == "-h" || args[0] == "--help")
}
