package safety

import (
	"testing"

	"github.com/GabboPenna/kx/internal/kube"
)

func TestEvaluateAllowsReadOnlyFanOut(t *testing.T) {
	decision := Evaluate(Plan{
		Selector: "@prod",
		Command:  []string{"get", "pods", "-A"},
		Contexts: []kube.Context{{Name: "prod-eu"}, {Name: "prod-us"}},
	})
	if !decision.Allowed || decision.NeedsConfirmation {
		t.Fatalf("expected read-only command to pass quietly: %#v", decision)
	}
}

func TestEvaluatePromptsForProdMutation(t *testing.T) {
	decision := Evaluate(Plan{
		Selector: "@prod",
		Command:  []string{"delete", "pod", "api"},
		Contexts: []kube.Context{{Name: "prod-eu"}},
		Tags: map[string]map[string]string{
			"prod-eu": {"env": "prod"},
		},
	})
	if !decision.Allowed || !decision.NeedsConfirmation {
		t.Fatalf("expected prod mutation prompt: %#v", decision)
	}
}

func TestEvaluateSkipsPromptWhenApproved(t *testing.T) {
	decision := Evaluate(Plan{
		Selector: "@prod",
		Command:  []string{"rollout", "restart", "deploy/api"},
		Contexts: []kube.Context{{Name: "prod-eu"}},
		Approved: true,
	})
	if !decision.Allowed || decision.NeedsConfirmation {
		t.Fatalf("expected approved mutation to pass: %#v", decision)
	}
}
