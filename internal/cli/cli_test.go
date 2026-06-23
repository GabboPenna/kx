package cli

import "testing"

func TestParseGlobalOptionsStopsAtKubectlCommand(t *testing.T) {
	opts, rest, err := parseGlobalOptions([]string{"@prod", "--parallel", "4", "apply", "-f", "app.yaml", "--dry-run=server"})
	if err != nil {
		t.Fatal(err)
	}
	if opts.Selector != "@prod" || opts.Parallel != 4 {
		t.Fatalf("unexpected options: %#v", opts)
	}
	want := []string{"apply", "-f", "app.yaml", "--dry-run=server"}
	if len(rest) != len(want) {
		t.Fatalf("expected %#v, got %#v", want, rest)
	}
	for i := range want {
		if rest[i] != want[i] {
			t.Fatalf("expected %#v, got %#v", want, rest)
		}
	}
}

func TestRemoveValueFlagSupportsEqualsAndSpace(t *testing.T) {
	value, rest := removeValueFlag([]string{"deploy/api", "--cols=context,ready", "-n", "payments"}, "--cols")
	if value != "context,ready" {
		t.Fatalf("unexpected value: %q", value)
	}
	want := []string{"deploy/api", "-n", "payments"}
	if len(rest) != len(want) {
		t.Fatalf("expected %#v, got %#v", want, rest)
	}
	for i := range want {
		if rest[i] != want[i] {
			t.Fatalf("expected %#v, got %#v", want, rest)
		}
	}
}

func TestSanitizeJSONDropsRuntimeNoise(t *testing.T) {
	obj := kubeAny{
		"kind": "Deployment",
		"metadata": map[string]any{
			"name":              "api",
			"uid":               "abc",
			"resourceVersion":   "123",
			"creationTimestamp": "2026-06-23T00:00:00Z",
			"managedFields":     []any{map[string]any{"manager": "kubectl"}},
		},
		"status": map[string]any{"readyReplicas": float64(2)},
		"spec":   map[string]any{"replicas": float64(2)},
	}
	clean := sanitizeJSON(obj).(map[string]any)
	if _, ok := clean["status"]; ok {
		t.Fatal("status should be removed")
	}
	meta := clean["metadata"].(map[string]any)
	for _, key := range []string{"uid", "resourceVersion", "creationTimestamp", "managedFields"} {
		if _, ok := meta[key]; ok {
			t.Fatalf("%s should be removed", key)
		}
	}
}
