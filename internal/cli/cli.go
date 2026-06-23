package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/GabboPenna/kx/internal/history"
	"github.com/GabboPenna/kx/internal/kube"
	"github.com/GabboPenna/kx/internal/output"
	"github.com/GabboPenna/kx/internal/safety"
	"github.com/GabboPenna/kx/internal/store"
)

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

type globalOptions struct {
	Selector    string
	Parallel    int
	Timeout     time.Duration
	FailFast    bool
	DryRun      bool
	JSONL       bool
	NoHeader    bool
	Yes         bool
	Canary      int
	ShowVersion bool
}

// Run is intentionally small: kx should feel like kubectl until the first @selector appears.
func Run(args []string, stdout io.Writer, stderr io.Writer) int {
	opts, rest, err := parseGlobalOptions(args)
	if err != nil {
		fmt.Fprintln(stderr, "kx:", err)
		return 2
	}

	if opts.ShowVersion {
		return runVersion(stdout, stderr)
	}

	if len(rest) == 0 {
		printHelp(stdout)
		return 0
	}

	switch rest[0] {
	case "help", "-h", "--help":
		printHelp(stdout)
		return 0
	case "ctx", "context", "contexts":
		return runContextCommand(rest[1:], stdout, stderr)
	case "history", "hist":
		return runHistoryCommand(rest[1:], stdout, stderr)
	case "doctor":
		return runDoctor(stdout, stderr)
	case "why", "explain":
		return runWhyCommand(opts, rest[1:], stdout, stderr)
	case "matrix":
		return runMatrixCommand(opts, rest[1:], stdout, stderr)
	case "diff":
		return runDiffCommand(opts, rest[1:], stdout, stderr)
	case "drift":
		return runDriftCommand(opts, rest[1:], stdout, stderr)
	case "logs", "log":
		return runLogsCommand(opts, rest[1:], stdout, stderr)
	case "events", "event":
		return runEventsCommand(opts, rest[1:], stdout, stderr)
	case "can":
		return runCanCommand(opts, rest[1:], stdout, stderr)
	case "access":
		return runAccessCommand(opts, rest[1:], stdout, stderr)
	case "completion", "completions":
		return runCompletionCommand(rest[1:], stdout, stderr)
	case "shell-init":
		return runShellInitCommand(rest[1:], stdout, stderr)
	case "prompt":
		return runPromptCommand(rest[1:], stdout, stderr)
	}

	return runKubectlLike(opts, rest, stdout, stderr)
}

func parseGlobalOptions(args []string) (globalOptions, []string, error) {
	opts := globalOptions{Parallel: 1}
	rest := make([]string, 0, len(args))

	for i := 0; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "@") && opts.Selector == "" {
			opts.Selector = arg
			continue
		}
		if arg == "--" {
			rest = append(rest, args[i+1:]...)
			break
		}
		switch {
		case arg == "--version" || arg == "version":
			opts.ShowVersion = true
		case arg == "--fail-fast":
			opts.FailFast = true
		case arg == "--dry-run":
			opts.DryRun = true
		case arg == "--jsonl":
			opts.JSONL = true
		case arg == "--no-header":
			opts.NoHeader = true
		case arg == "--yes" || arg == "-y":
			opts.Yes = true
		case arg == "--parallel":
			i++
			if i >= len(args) {
				return opts, nil, errors.New("--parallel needs a value")
			}
			n, err := strconv.Atoi(args[i])
			if err != nil || n < 1 {
				return opts, nil, errors.New("--parallel must be a positive integer")
			}
			opts.Parallel = n
		case strings.HasPrefix(arg, "--parallel="):
			n, err := strconv.Atoi(strings.TrimPrefix(arg, "--parallel="))
			if err != nil || n < 1 {
				return opts, nil, errors.New("--parallel must be a positive integer")
			}
			opts.Parallel = n
		case arg == "--timeout":
			i++
			if i >= len(args) {
				return opts, nil, errors.New("--timeout needs a value like 20s or 2m")
			}
			d, err := time.ParseDuration(args[i])
			if err != nil {
				return opts, nil, fmt.Errorf("invalid --timeout: %w", err)
			}
			opts.Timeout = d
		case strings.HasPrefix(arg, "--timeout="):
			d, err := time.ParseDuration(strings.TrimPrefix(arg, "--timeout="))
			if err != nil {
				return opts, nil, fmt.Errorf("invalid --timeout: %w", err)
			}
			opts.Timeout = d
		case arg == "--canary":
			i++
			if i >= len(args) {
				return opts, nil, errors.New("--canary needs a value")
			}
			n, err := strconv.Atoi(args[i])
			if err != nil || n < 1 {
				return opts, nil, errors.New("--canary must be a positive integer")
			}
			opts.Canary = n
		case strings.HasPrefix(arg, "--canary="):
			n, err := strconv.Atoi(strings.TrimPrefix(arg, "--canary="))
			if err != nil || n < 1 {
				return opts, nil, errors.New("--canary must be a positive integer")
			}
			opts.Canary = n
		default:
			rest = append(rest, args[i:]...)
			return opts, rest, nil
		}
	}

	if opts.Parallel == 0 {
		opts.Parallel = runtime.NumCPU()
	}

	return opts, rest, nil
}

func runKubectlLike(opts globalOptions, kubectlArgs []string, stdout io.Writer, stderr io.Writer) int {
	kubectlPath, err := exec.LookPath("kubectl")
	if err != nil {
		fmt.Fprintln(stderr, "kx: kubectl was not found in PATH")
		return 127
	}

	if opts.Selector == "" {
		return runSinglePassThrough(kubectlPath, kubectlArgs, stdout, stderr)
	}

	if containsContextFlag(kubectlArgs) {
		fmt.Fprintln(stderr, "kx: do not mix @selectors with kubectl --context; let kx own the context fan-out")
		return 2
	}

	kubeState, err := kube.Load(kubectlPath)
	if err != nil {
		fmt.Fprintln(stderr, "kx:", err)
		return 1
	}

	meta, err := store.Load()
	if err != nil {
		fmt.Fprintln(stderr, "kx:", err)
		return 1
	}

	matches, err := kube.MatchSelector(opts.Selector, kubeState, meta.ContextTags)
	if err != nil {
		fmt.Fprintln(stderr, "kx:", err)
		return 2
	}
	if len(matches) == 0 {
		fmt.Fprintf(stderr, "kx: selector %q matched no contexts\n", opts.Selector)
		return 1
	}
	if opts.Canary > 0 && opts.Canary < len(matches) {
		matches = matches[:opts.Canary]
	}

	plan := safety.Plan{
		Selector: opts.Selector,
		Command:  kubectlArgs,
		Contexts: matches,
		Tags:     meta.ContextTags,
		DryRun:   opts.DryRun,
		Approved: opts.Yes || os.Getenv("KX_FORCE") == "1",
	}
	decision := safety.Evaluate(plan)
	if !decision.Allowed {
		fmt.Fprintln(stderr, decision.Message)
		return 2
	}
	if decision.NeedsConfirmation && !confirm(stderr, decision.Message) {
		fmt.Fprintln(stderr, "kx: aborted")
		return 130
	}

	if opts.DryRun {
		output.PrintPlan(stdout, opts.Selector, kubectlArgs, matches)
		return 0
	}

	started := time.Now()
	results := runFanOut(kubectlPath, kubectlArgs, matches, opts)
	entry := history.EntryFromResults(started, opts.Selector, kubectlArgs, results)
	_ = history.Append(entry)

	if opts.JSONL {
		output.PrintJSONL(stdout, results)
	} else {
		output.PrintGrouped(stdout, results, output.GroupOptions{NoHeader: opts.NoHeader})
	}

	if hasFailures(results) {
		return 1
	}
	return 0
}

func runSinglePassThrough(kubectlPath string, args []string, stdout io.Writer, stderr io.Writer) int {
	cmd := exec.Command(kubectlPath, args...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.Stdin = os.Stdin
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return exitErr.ExitCode()
		}
		fmt.Fprintln(stderr, "kx:", err)
		return 1
	}
	return 0
}

func runFanOut(kubectlPath string, args []string, contexts []kube.Context, opts globalOptions) []history.Result {
	type job struct {
		index int
		ctx   kube.Context
	}

	workers := opts.Parallel
	if workers > len(contexts) {
		workers = len(contexts)
	}
	jobs := make(chan job)
	results := make([]history.Result, len(contexts))
	cancelled := make(chan struct{})
	var cancelOnce sync.Once
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				select {
				case <-cancelled:
					results[j.index] = history.Result{
						Context:  j.ctx.Name,
						ExitCode: 130,
						Stderr:   "skipped after fail-fast",
					}
					continue
				default:
				}
				results[j.index] = runOneContext(kubectlPath, args, j.ctx.Name, opts.Timeout)
				if opts.FailFast && results[j.index].ExitCode != 0 {
					cancelOnce.Do(func() {
						close(cancelled)
					})
				}
			}
		}()
	}

	for i, ctx := range contexts {
		jobs <- job{index: i, ctx: ctx}
	}
	close(jobs)
	wg.Wait()

	return results
}

func runOneContext(kubectlPath string, args []string, contextName string, timeout time.Duration) history.Result {
	started := time.Now()
	ctx := context.Background()
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	fullArgs := append([]string{"--context", contextName}, args...)
	cmd := exec.CommandContext(ctx, kubectlPath, fullArgs...)
	var out strings.Builder
	var errOut strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	cmd.Stdin = os.Stdin

	exitCode := 0
	err := cmd.Run()
	if ctx.Err() == context.DeadlineExceeded {
		exitCode = 124
		errOut.WriteString("kx: command timed out\n")
	} else if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else {
			exitCode = 1
			errOut.WriteString(err.Error())
			errOut.WriteString("\n")
		}
	}

	return history.Result{
		Context:  contextName,
		ExitCode: exitCode,
		Stdout:   out.String(),
		Stderr:   errOut.String(),
		Duration: time.Since(started),
	}
}

func runContextCommand(args []string, stdout io.Writer, stderr io.Writer) int {
	kubectlPath, err := exec.LookPath("kubectl")
	if err != nil {
		fmt.Fprintln(stderr, "kx: kubectl was not found in PATH")
		return 127
	}
	kubeState, err := kube.Load(kubectlPath)
	if err != nil {
		fmt.Fprintln(stderr, "kx:", err)
		return 1
	}
	meta, err := store.Load()
	if err != nil {
		fmt.Fprintln(stderr, "kx:", err)
		return 1
	}

	if len(args) == 0 || args[0] == "ls" || args[0] == "list" {
		output.PrintContexts(stdout, kubeState, meta.ContextTags)
		return 0
	}

	switch args[0] {
	case "tag":
		if len(args) < 3 {
			fmt.Fprintln(stderr, "usage: kx ctx tag <context> key=value [key=value...]")
			return 2
		}
		name := args[1]
		if !kubeState.HasContext(name) {
			fmt.Fprintf(stderr, "kx: unknown context %q\n", name)
			return 1
		}
		if meta.ContextTags[name] == nil {
			meta.ContextTags[name] = map[string]string{}
		}
		for _, pair := range args[2:] {
			k, v, ok := strings.Cut(pair, "=")
			if !ok || k == "" {
				fmt.Fprintf(stderr, "kx: invalid tag %q, expected key=value\n", pair)
				return 2
			}
			meta.ContextTags[name][k] = v
		}
		if err := store.Save(meta); err != nil {
			fmt.Fprintln(stderr, "kx:", err)
			return 1
		}
		fmt.Fprintf(stdout, "tagged %s\n", name)
		return 0
	case "untag":
		if len(args) < 3 {
			fmt.Fprintln(stderr, "usage: kx ctx untag <context> key [key...]")
			return 2
		}
		name := args[1]
		for _, key := range args[2:] {
			delete(meta.ContextTags[name], key)
		}
		if err := store.Save(meta); err != nil {
			fmt.Fprintln(stderr, "kx:", err)
			return 1
		}
		fmt.Fprintf(stdout, "untagged %s\n", name)
		return 0
	case "show":
		if len(args) != 2 {
			fmt.Fprintln(stderr, "usage: kx ctx show <context>")
			return 2
		}
		ctx, ok := kubeState.ContextByName(args[1])
		if !ok {
			fmt.Fprintf(stderr, "kx: unknown context %q\n", args[1])
			return 1
		}
		output.PrintContext(stdout, ctx, meta.ContextTags[ctx.Name], kubeState.CurrentContext == ctx.Name)
		return 0
	case "where":
		if len(args) != 2 {
			fmt.Fprintln(stderr, "usage: kx ctx where <selector>")
			return 2
		}
		matches, err := kube.MatchSelector(args[1], kubeState, meta.ContextTags)
		if err != nil {
			fmt.Fprintln(stderr, "kx:", err)
			return 2
		}
		output.PrintContextNames(stdout, matches)
		return 0
	case "scan":
		return runContextScanCommand(args[1:], kubeState, meta, kubectlPath, stdout, stderr)
	case "clean":
		return runContextCleanCommand(args[1:], kubeState, meta, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "kx: unknown ctx command %q\n", args[0])
		return 2
	}
}

func runHistoryCommand(args []string, stdout io.Writer, stderr io.Writer) int {
	if len(args) == 0 || args[0] == "ls" || args[0] == "list" {
		entries, err := history.LoadRecent(20)
		if err != nil {
			fmt.Fprintln(stderr, "kx:", err)
			return 1
		}
		output.PrintHistory(stdout, entries)
		return 0
	}

	fmt.Fprintf(stderr, "kx: unknown history command %q\n", args[0])
	return 2
}

func runVersion(stdout io.Writer, stderr io.Writer) int {
	fmt.Fprintf(stdout, "kx %s\n", version)
	fmt.Fprintf(stdout, "commit: %s\n", commit)
	fmt.Fprintf(stdout, "built: %s\n", date)
	kubectlPath, err := exec.LookPath("kubectl")
	if err != nil {
		fmt.Fprintln(stderr, "kubectl: not found")
		return 0
	}
	cmd := exec.Command(kubectlPath, "version", "--client")
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintln(stderr, "kubectl:", err)
	}
	return 0
}

func runDoctor(stdout io.Writer, stderr io.Writer) int {
	kubectlPath, err := exec.LookPath("kubectl")
	if err != nil {
		fmt.Fprintln(stderr, "kubectl: missing")
		return 1
	}
	fmt.Fprintf(stdout, "kubectl: %s\n", kubectlPath)
	if _, err := store.Dir(); err != nil {
		fmt.Fprintln(stderr, "kx home:", err)
		return 1
	}
	dir, _ := store.Dir()
	fmt.Fprintf(stdout, "kx home: %s\n", dir)
	if _, err := kube.Load(kubectlPath); err != nil {
		fmt.Fprintln(stderr, "kubeconfig:", err)
		return 1
	}
	fmt.Fprintln(stdout, "kubeconfig: ok")
	return 0
}

func confirm(stderr io.Writer, message string) bool {
	fmt.Fprintln(stderr, message)
	fmt.Fprint(stderr, "Type 'run' to continue: ")
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		return false
	}
	return scanner.Text() == "run"
}

func containsContextFlag(args []string) bool {
	for i, arg := range args {
		if arg == "--context" && i+1 < len(args) {
			return true
		}
		if strings.HasPrefix(arg, "--context=") {
			return true
		}
	}
	return false
}

func hasFailures(results []history.Result) bool {
	for _, result := range results {
		if result.ExitCode != 0 {
			return true
		}
	}
	return false
}

func printHelp(stdout io.Writer) {
	fmt.Fprint(stdout, `kx - kubectl with context algebra

Usage:
  kx <kubectl args...>
  kx @selector [kx options] <kubectl args...>
  kx ctx ls
  kx ctx tag <context> key=value [key=value...]
  kx history
  kx why <resource> [-n namespace] [--deep]
  kx matrix <resource> [-n namespace] [--cols context,ready,image]
  kx diff <resource> [-n namespace]
  kx logs <resource> [-n namespace] [--grep pattern]
  kx events [-n namespace] [--warnings]
  kx access [-n namespace]

Selectors:
  @all                 every context in kubeconfig order
  @current             the current kubectl context
  @prod                fuzzy match context name or tags
  @prod.eu             match all facets
  @env=prod            match a local kx tag
  @team:payments       same as @team=payments
  @/regex/             regular expression against context names
  @prod,@staging       union of selectors

kx options before the kubectl command:
  --parallel N         run N contexts at a time
  --timeout 20s        kill slow context calls
  --fail-fast          skip pending work after the first failure
  --dry-run            print the context plan without calling kubectl
  --canary N           run only the first N matched contexts
  --jsonl              print machine-friendly output
  --no-header          suppress grouped output headers
  --yes, -y            approve safety prompts

Examples:
  kx get pods -A
  kx @prod --parallel 4 get deploy -n payments
  kx @prod why deploy/api -n payments --deep
  kx @prod matrix deploy/api -n payments --cols context,ready,image,rollout
  kx @prod logs deploy/api -n payments --since 15m --grep error
  kx @env=prod --dry-run delete pod old-api -n payments
  kx ctx tag aks-prod-weu env=prod region=eu team=payments risk=high
`)
}
