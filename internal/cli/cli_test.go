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
