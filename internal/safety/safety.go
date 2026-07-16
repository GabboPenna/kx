package safety

import (
	"fmt"
	"strings"

	"github.com/GabboPenna/kx/internal/kube"
)

type Plan struct {
	Selector string
	Command  []string
	Contexts []kube.Context
	Tags     map[string]map[string]string
	DryRun   bool
	Approved bool
}

type Decision struct {
	Allowed           bool
	NeedsConfirmation bool
	Message           string
}

// Evaluate is deliberately boring and conservative. The shell should be fast, not reckless.
func Evaluate(plan Plan) Decision {
	if len(plan.Command) == 0 {
		return Decision{Allowed: false, Message: "kx: no kubectl command provided"}
	}
	if plan.DryRun {
		return Decision{Allowed: true}
	}
	if !isMutating(plan.Command) {
		return Decision{Allowed: true}
	}

	prodLike := prodLikeContexts(plan)
	if len(prodLike) == 0 && len(plan.Contexts) == 1 {
		return Decision{Allowed: true}
	}

	if plan.Approved {
		return Decision{Allowed: true}
	}

	if len(prodLike) > 0 {
		return Decision{
			Allowed:           true,
			NeedsConfirmation: true,
			Message:           fmt.Sprintf("kx: mutating %d prod-like context(s): %s", len(prodLike), strings.Join(prodLike, ", ")),
		}
	}

	if len(plan.Contexts) > 1 {
		return Decision{
			Allowed:           true,
			NeedsConfirmation: true,
			Message:           fmt.Sprintf("kx: mutating %d contexts", len(plan.Contexts)),
		}
	}

	return Decision{Allowed: true}
}

func isMutating(args []string) bool {
	verb, rest := kubectlVerb(args)
	if verb == "" {
		return false
	}
	switch verb {
	case "apply", "create", "delete", "replace", "patch", "scale", "autoscale",
		"edit", "expose", "run", "debug", "cordon", "uncordon", "drain",
		"taint", "annotate", "label", "config":
		return true
	case "rollout":
		return len(rest) > 0 && rest[0] != "status" && rest[0] != "history"
	case "set", "certificate":
		return true
	case "auth":
		return len(rest) > 0 && rest[0] == "reconcile"
	}
	return false
}

// kubectl accepts persistent flags before the command (for example
// "kubectl -n payments delete pod api"). Safety must find the actual verb,
// otherwise moving a flag can accidentally turn a guarded mutation into a
// command that appears read-only.
func kubectlVerb(args []string) (string, []string) {
	flagsWithValue := map[string]bool{
		"--as": true, "--as-group": true, "--cache-dir": true,
		"--certificate-authority": true, "--client-certificate": true,
		"--client-key": true, "--cluster": true, "--context": true,
		"--kubeconfig": true, "--namespace": true, "-n": true,
		"--password": true, "--profile": true, "--profile-output": true,
		"--request-timeout": true, "--server": true, "-s": true,
		"--tls-server-name": true, "--token": true, "--user": true,
		"--username": true, "--v": true, "--vmodule": true,
	}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			if i+1 < len(args) {
				return strings.ToLower(args[i+1]), args[i+2:]
			}
			return "", nil
		}
		if !strings.HasPrefix(arg, "-") {
			return strings.ToLower(arg), args[i+1:]
		}
		name := arg
		if before, _, ok := strings.Cut(arg, "="); ok {
			name = before
		}
		if flagsWithValue[name] && !strings.Contains(arg, "=") {
			i++
		}
	}
	return "", nil
}

func prodLikeContexts(plan Plan) []string {
	var out []string
	for _, ctx := range plan.Contexts {
		tags := plan.Tags[ctx.Name]
		if strings.EqualFold(tags["env"], "prod") ||
			strings.EqualFold(tags["risk"], "high") ||
			strings.Contains(strings.ToLower(ctx.Name), "prod") ||
			strings.Contains(strings.ToLower(plan.Selector), "prod") {
			out = append(out, ctx.Name)
		}
	}
	return out
}
