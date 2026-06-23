package output

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/GabboPenna/kx/internal/history"
	"github.com/GabboPenna/kx/internal/kube"
)

type GroupOptions struct {
	NoHeader bool
}

func PrintGrouped(w io.Writer, results []history.Result, opts GroupOptions) {
	for _, result := range results {
		if !opts.NoHeader {
			status := "ok"
			if result.ExitCode != 0 {
				status = fmt.Sprintf("exit=%d", result.ExitCode)
			}
			fmt.Fprintf(w, "### %s (%s, %s)\n", result.Context, status, result.Duration.Round(time.Millisecond))
		}
		if result.Stdout != "" {
			fmt.Fprint(w, result.Stdout)
			if !strings.HasSuffix(result.Stdout, "\n") {
				fmt.Fprintln(w)
			}
		}
		if result.Stderr != "" {
			fmt.Fprint(w, result.Stderr)
			if !strings.HasSuffix(result.Stderr, "\n") {
				fmt.Fprintln(w)
			}
		}
	}
}

func PrintJSONL(w io.Writer, results []history.Result) {
	for _, result := range results {
		lines := splitLines(result.Stdout)
		if len(lines) == 0 {
			lines = []string{""}
		}
		for _, line := range lines {
			row := map[string]any{
				"context":  result.Context,
				"exitCode": result.ExitCode,
				"stream":   "stdout",
				"line":     line,
			}
			writeJSON(w, row)
		}
		for _, line := range splitLines(result.Stderr) {
			row := map[string]any{
				"context":  result.Context,
				"exitCode": result.ExitCode,
				"stream":   "stderr",
				"line":     line,
			}
			writeJSON(w, row)
		}
	}
}

func PrintPlan(w io.Writer, selector string, command []string, contexts []kube.Context) {
	fmt.Fprintf(w, "selector: %s\n", selector)
	fmt.Fprintf(w, "command: kubectl %s\n", strings.Join(command, " "))
	fmt.Fprintln(w, "contexts:")
	for _, ctx := range contexts {
		fmt.Fprintf(w, "  - %s\n", ctx.Name)
	}
}

func PrintContexts(w io.Writer, state kube.State, tags map[string]map[string]string) {
	fmt.Fprintf(w, "%-2s %-36s %-24s %-20s %s\n", "", "CONTEXT", "NAMESPACE", "CLUSTER", "TAGS")
	for _, ctx := range state.Contexts {
		current := ""
		if ctx.Name == state.CurrentContext {
			current = "*"
		}
		fmt.Fprintf(w, "%-2s %-36s %-24s %-20s %s\n", current, ctx.Name, valueOrDash(ctx.Namespace), valueOrDash(ctx.Cluster), formatTags(tags[ctx.Name]))
	}
}

func PrintContext(w io.Writer, ctx kube.Context, tags map[string]string, current bool) {
	fmt.Fprintf(w, "name: %s\n", ctx.Name)
	fmt.Fprintf(w, "current: %v\n", current)
	fmt.Fprintf(w, "cluster: %s\n", valueOrDash(ctx.Cluster))
	fmt.Fprintf(w, "user: %s\n", valueOrDash(ctx.User))
	fmt.Fprintf(w, "namespace: %s\n", valueOrDash(ctx.Namespace))
	fmt.Fprintf(w, "tags: %s\n", formatTags(tags))
}

func PrintContextNames(w io.Writer, contexts []kube.Context) {
	for _, ctx := range contexts {
		fmt.Fprintln(w, ctx.Name)
	}
}

func PrintHistory(w io.Writer, entries []history.Entry) {
	fmt.Fprintf(w, "%-30s %-14s %-8s %s\n", "ID", "SELECTOR", "STATUS", "COMMAND")
	for _, entry := range entries {
		status := "ok"
		for _, result := range entry.Results {
			if result.ExitCode != 0 {
				status = "failed"
				break
			}
		}
		fmt.Fprintf(w, "%-30s %-14s %-8s kubectl %s\n", entry.ID, entry.Selector, status, strings.Join(entry.Command, " "))
	}
}

func writeJSON(w io.Writer, value any) {
	data, _ := json.Marshal(value)
	fmt.Fprintln(w, string(data))
}

func splitLines(s string) []string {
	s = strings.TrimSuffix(s, "\n")
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

func formatTags(tags map[string]string) string {
	if len(tags) == 0 {
		return "-"
	}
	keys := make([]string, 0, len(tags))
	for key := range tags {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+tags[key])
	}
	return strings.Join(parts, ",")
}

func valueOrDash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}
