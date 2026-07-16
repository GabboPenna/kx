package cli

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/GabboPenna/kx/internal/history"
)

func TestCommandHelpDoesNotNeedKubectl(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := Run([]string{"matrix", "--help"}, &stdout, &stderr)
	if code != 0 || !strings.Contains(stdout.String(), "Usage: kx [@selector] matrix") {
		t.Fatalf("unexpected help result code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestUnknownHelpTopicIsActionable(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := Run([]string{"help", "wat"}, &stdout, &stderr)
	if code != 2 || !strings.Contains(stderr.String(), "try: kx help selectors") {
		t.Fatalf("unexpected help result code=%d stderr=%q", code, stderr.String())
	}
}

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

func TestMatrixNamespaceMode(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want bool
	}{
		{name: "empty means current namespace overview", args: nil, want: true},
		{name: "namespace flag means namespace overview", args: []string{"-n", "payments"}, want: true},
		{name: "selector flag still means namespace overview", args: []string{"-n", "payments", "-l", "app=api"}, want: true},
		{name: "resource target means resource mode", args: []string{"deploy/api", "-n", "payments"}, want: false},
		{name: "resource list means resource mode", args: []string{"pods", "-A"}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matrixNamespaceMode(tt.args); got != tt.want {
				t.Fatalf("matrixNamespaceMode(%#v) = %v, want %v", tt.args, got, tt.want)
			}
		})
	}
}

func TestPrintMatrixHasHeaderAndFitsLongCells(t *testing.T) {
	longImage := "ghcr.io/acme/payments/api:" + strings.Repeat("0123456789", 12)
	var out strings.Builder
	printMatrix(&out, matrixPrintOptions{
		Cols:     splitCSV("context,namespace,kind,name,status,image"),
		Mode:     "namespace",
		Scope:    "namespace/payments",
		Contexts: 1,
		Rows: []objectSummary{{
			Context:   "prod-eu-west-1",
			Namespace: "payments",
			Kind:      "Deployment",
			Name:      "api",
			Status:    "Ready",
			Image:     longImage,
		}},
	})
	text := out.String()
	for _, want := range []string{"kx matrix", "mode: namespace", "CONTEXT", "STATUS", "..."} {
		if !strings.Contains(text, want) {
			t.Fatalf("matrix output missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, longImage) {
		t.Fatalf("matrix output should fit long cells:\n%s", text)
	}
}

func TestDefaultMatrixColumnsAreServiceAware(t *testing.T) {
	cols := splitCSV(defaultMatrixColumns([]string{"svc", "-n", "payments"}, false))
	for _, want := range []string{"type", "cluster-ip", "external-ip", "ports", "endpoints", "selector"} {
		if !needsColumn(cols, want) {
			t.Fatalf("service matrix columns missing %q: %#v", want, cols)
		}
	}
	for _, unwanted := range []string{"ready", "image", "replicas", "rollout"} {
		if needsColumn(cols, unwanted) {
			t.Fatalf("service matrix columns should not contain %q: %#v", unwanted, cols)
		}
	}
}

func TestSummarizeServiceKindAwareFields(t *testing.T) {
	obj := kubeAny{
		"kind": "Service",
		"metadata": map[string]any{
			"name":      "api",
			"namespace": "payments",
		},
		"spec": map[string]any{
			"type":      "LoadBalancer",
			"clusterIP": "10.0.0.42",
			"selector":  map[string]any{"app": "api"},
			"ports": []any{
				map[string]any{"name": "http", "port": float64(80), "protocol": "TCP"},
			},
		},
		"status": map[string]any{
			"loadBalancer": map[string]any{
				"ingress": []any{map[string]any{"ip": "35.1.2.3"}},
			},
		},
	}
	row := summarizeObject("prod", obj)
	if row.Type != "LoadBalancer" || row.Status != "Exposed" || row.ClusterIP != "10.0.0.42" || row.ExternalIP != "35.1.2.3" {
		t.Fatalf("unexpected service summary: %#v", row)
	}
	if row.Ports != "http=80/TCP" || row.Selector != "app=api" {
		t.Fatalf("unexpected service ports/selector: %#v", row)
	}
	facts := summaryFacts(row)
	for _, fact := range facts {
		if fact[0] == "image" || fact[0] == "replicas" || fact[0] == "ready" {
			t.Fatalf("service facts should be kind-aware, got %#v", facts)
		}
	}
}

func TestEndpointSummaryFromSlicesCountsReadyAddresses(t *testing.T) {
	root := kubeAny{
		"items": []any{
			map[string]any{
				"endpoints": []any{
					map[string]any{
						"addresses":  []any{"10.2.0.1", "10.2.0.2"},
						"conditions": map[string]any{"ready": true},
					},
					map[string]any{
						"addresses":  []any{"10.2.0.3"},
						"conditions": map[string]any{"ready": false},
					},
				},
			},
		},
	}
	ready, total := endpointSummaryFromSlices(root)
	if ready != 2 || total != 3 {
		t.Fatalf("endpointSummaryFromSlices() = %d/%d, want 2/3", ready, total)
	}
}

func TestStatusStringPodWaitingReason(t *testing.T) {
	obj := kubeAny{
		"kind": "Pod",
		"status": map[string]any{
			"phase": "Pending",
			"containerStatuses": []any{
				map[string]any{
					"state": map[string]any{
						"waiting": map[string]any{"reason": "ImagePullBackOff"},
					},
				},
			},
		},
	}
	if got := statusString(obj); got != "ImagePullBackOff" {
		t.Fatalf("statusString() = %q, want ImagePullBackOff", got)
	}
}

func TestStatusStringRunningPodNotReady(t *testing.T) {
	obj := kubeAny{
		"kind": "Pod",
		"status": map[string]any{
			"phase": "Running",
			"containerStatuses": []any{
				map[string]any{"ready": true},
				map[string]any{"ready": false},
			},
		},
	}
	if got := statusString(obj); got != "NotReady" {
		t.Fatalf("statusString() = %q, want NotReady", got)
	}
}

func TestPrintPrefixedIncludesStreamColumn(t *testing.T) {
	var out strings.Builder
	printPrefixed(&out, []history.Result{
		{Context: "prod", Stdout: "ok\n", Stderr: "warn\n", Duration: time.Millisecond},
	})
	text := out.String()
	if !strings.Contains(text, "| out | ok") || !strings.Contains(text, "| err | warn") {
		t.Fatalf("unexpected prefixed output:\n%s", text)
	}
}
