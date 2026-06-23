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
	if len(args) == 0 {
		return false
	}
	verb := args[0]
	switch verb {
	case "apply", "create", "delete", "replace", "patch", "scale", "edit", "cordon", "uncordon", "drain", "taint", "annotate", "label":
		return true
	case "rollout":
		return len(args) > 1 && args[1] != "status" && args[1] != "history"
	case "set":
		return true
	}
	return false
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
