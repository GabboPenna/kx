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

func TestEvaluateFindsMutationAfterKubectlGlobalFlags(t *testing.T) {
	for _, command := range [][]string{
		{"-n", "payments", "delete", "pod", "api"},
		{"--namespace=payments", "rollout", "restart", "deploy/api"},
		{"--request-timeout", "10s", "apply", "-f", "app.yaml"},
	} {
		decision := Evaluate(Plan{
			Selector: "@all",
			Command:  command,
			Contexts: []kube.Context{{Name: "dev"}, {Name: "stage"}},
		})
		if !decision.NeedsConfirmation {
			t.Fatalf("expected %#v to require confirmation: %#v", command, decision)
		}
	}
}

func TestEvaluateAllowsReadOnlyCommandAfterGlobalFlags(t *testing.T) {
	decision := Evaluate(Plan{
		Selector: "@all",
		Command:  []string{"-n", "payments", "get", "pods"},
		Contexts: []kube.Context{{Name: "dev"}, {Name: "stage"}},
	})
	if !decision.Allowed || decision.NeedsConfirmation {
		t.Fatalf("expected read-only command to pass quietly: %#v", decision)
	}
}

func TestMutatingCoversKubectlWorkloadCreators(t *testing.T) {
	for _, command := range [][]string{
		{"run", "shell", "--image=busybox"},
		{"expose", "deployment", "api"},
		{"autoscale", "deployment", "api"},
		{"auth", "reconcile", "-f", "rbac.yaml"},
		{"certificate", "approve", "node-csr"},
	} {
		if !isMutating(command) {
			t.Errorf("expected %#v to be classified as mutating", command)
		}
	}
}
