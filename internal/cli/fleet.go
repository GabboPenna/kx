package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/GabboPenna/kx/internal/history"
	"github.com/GabboPenna/kx/internal/kube"
	"github.com/GabboPenna/kx/internal/store"
)

type kubeAny map[string]any

type objectSummary struct {
	Context      string
	Kind         string
	Name         string
	Namespace    string
	Status       string
	Ready        string
	Image        string
	Replicas     string
	Rollout      string
	Age          string
	Warnings     string
	Hash         string
	Type         string
	ClusterIP    string
	ExternalIP   string
	Ports        string
	Selector     string
	Endpoints    string
	Node         string
	PodIP        string
	Restarts     string
	Class        string
	Hosts        string
	Address      string
	Schedule     string
	Suspend      string
	LastSchedule string
	Completions  string
	Succeeded    string
	Failed       string
	Active       string
	Roles        string
	Version      string
	InternalIP   string
	Capacity     string
	AccessModes  string
	Volume       string
	StorageClass string
	Data         string
	Detail       string
	Network      string
}

type eventRow struct {
	Time      time.Time
	Type      string
	Reason    string
	Object    string
	Namespace string
	Message   string
	Count     string
}

const namespaceMatrixResources = "deployments,statefulsets,daemonsets,pods,services,ingresses,jobs,cronjobs"

func runWhyCommand(opts globalOptions, args []string, stdout io.Writer, stderr io.Writer) int {
	kubectlPath, contexts, err := resolveCommandContexts(opts)
	if err != nil {
		fmt.Fprintln(stderr, "kx:", err)
		return 1
	}
	deep, args := removeBoolFlag(args, "--deep")
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: kx [@selector] why <resource> [-n namespace] [--deep]")
		return 2
	}
	results := runContextJobs(contexts, opts, func(ctx kube.Context) history.Result {
		started := time.Now()
		text, exitCode := whyOne(kubectlPath, ctx.Name, args, deep)
		return history.Result{Context: ctx.Name, ExitCode: exitCode, Stdout: text, Duration: time.Since(started)}
	})
	printGroupedSpecial(stdout, "why", results)
	return exitFromResults(results)
}

func whyOne(kubectlPath, contextName string, args []string, deep bool) (string, int) {
	obj, stderr, err := getObject(kubectlPath, contextName, args)
	if err != nil {
		return strings.TrimSpace(stderr) + "\n", 1
	}
	s := summarizeObject(contextName, obj)
	if s.Kind == "Service" {
		s.Endpoints = serviceEndpointSummary(kubectlPath, contextName, s.Namespace, s.Name)
		s.Detail = detailString(s)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "target: %s/%s", strings.ToLower(s.Kind), s.Name)
	if s.Namespace != "" {
		fmt.Fprintf(&b, " namespace=%s", s.Namespace)
	}
	fmt.Fprintln(&b)
	for _, fact := range summaryFacts(s) {
		fmt.Fprintf(&b, "%s: %s\n", fact[0], dash(fact[1]))
	}

	if len(objectConditions(obj)) > 0 {
		fmt.Fprintln(&b, "conditions:")
		for _, line := range objectConditions(obj) {
			fmt.Fprintf(&b, "  %s\n", line)
		}
	}
	if owners := ownerChain(obj); owners != "" {
		fmt.Fprintf(&b, "owners: %s\n", owners)
	}
	if rolloutTarget := rolloutResource(obj); rolloutTarget != "" {
		out, _, code := runKubectlCapture(kubectlPath, contextName, append([]string{"rollout", "status", rolloutTarget}, append(namespaceArgs(args), "--watch=false")...), 20*time.Second)
		if code == 0 && strings.TrimSpace(out) != "" {
			fmt.Fprintf(&b, "rollout: %s\n", oneLine(out))
		}
	}
	warnings := warningEvents(kubectlPath, contextName, obj, namespaceArgs(args))
	if strings.TrimSpace(warnings) != "" {
		fmt.Fprintln(&b, "warnings:")
		for _, line := range tailLines(warnings, 8) {
			fmt.Fprintf(&b, "  %s\n", line)
		}
	}
	if deep {
		fmt.Fprintln(&b, "deep:")
		describe, describeErr, describeCode := runKubectlCapture(kubectlPath, contextName, append([]string{"describe"}, args...), 30*time.Second)
		if describeCode == 0 {
			for _, line := range tailLines(describe, 60) {
				fmt.Fprintf(&b, "  %s\n", line)
			}
		} else if strings.TrimSpace(describeErr) != "" {
			fmt.Fprintf(&b, "  describe failed: %s\n", oneLine(describeErr))
		}
		if strings.EqualFold(s.Kind, "Pod") {
			logArgs := append([]string{"logs", s.Name}, namespaceArgs(args)...)
			logArgs = append(logArgs, "--all-containers=true", "--tail=80")
			logs, logErr, logCode := runKubectlCapture(kubectlPath, contextName, logArgs, 30*time.Second)
			if logCode == 0 && strings.TrimSpace(logs) != "" {
				fmt.Fprintln(&b, "logs:")
				for _, line := range tailLines(logs, 40) {
					fmt.Fprintf(&b, "  %s\n", line)
				}
			} else if strings.TrimSpace(logErr) != "" {
				fmt.Fprintf(&b, "  logs failed: %s\n", oneLine(logErr))
			}
		}
	}
	return b.String(), 0
}

func runMatrixCommand(opts globalOptions, args []string, stdout io.Writer, stderr io.Writer) int {
	colsValue, args := removeValueFlag(args, "--cols")
	resourcesValue, args := removeValueFlag(args, "--resources")
	namespaceMode := matrixNamespaceMode(args)
	if colsValue == "" {
		colsValue = defaultMatrixColumns(args, namespaceMode)
	}
	cols := splitCSV(colsValue)
	if len(args) == 0 && !namespaceMode {
		fmt.Fprintln(stderr, "usage: kx [@selector] matrix [resource] [-n namespace] [--resources deployments,pods] [--cols context,ready,image]")
		return 2
	}
	kubectlPath, contexts, err := resolveCommandContexts(opts)
	if err != nil {
		fmt.Fprintln(stderr, "kx:", err)
		return 1
	}

	var rows []objectSummary
	var mu sync.Mutex
	results := runContextJobs(contexts, opts, func(ctx kube.Context) history.Result {
		started := time.Now()
		var local []objectSummary
		var stderr string
		var failed bool
		if namespaceMode {
			local, stderr, failed = namespaceMatrixRows(kubectlPath, ctx.Name, args, splitCSV(defaultIfEmpty(resourcesValue, namespaceMatrixResources)), cols)
		} else {
			obj, errOut, err := getObject(kubectlPath, ctx.Name, args)
			if err != nil {
				return history.Result{Context: ctx.Name, ExitCode: 1, Stderr: errOut, Duration: time.Since(started)}
			}
			local = summarizeObjects(ctx.Name, obj)
			enrichMatrixRows(kubectlPath, ctx.Name, args, obj, local, cols)
		}
		mu.Lock()
		rows = append(rows, local...)
		mu.Unlock()
		exitCode := 0
		if failed {
			exitCode = 1
		}
		return history.Result{Context: ctx.Name, ExitCode: exitCode, Stderr: stderr, Duration: time.Since(started)}
	})
	if hasFailures(results) {
		printGroupedSpecial(stderr, "matrix errors", results)
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].Context != rows[j].Context {
			return rows[i].Context < rows[j].Context
		}
		if rows[i].Namespace != rows[j].Namespace {
			return rows[i].Namespace < rows[j].Namespace
		}
		if kindRank(rows[i].Kind) != kindRank(rows[j].Kind) {
			return kindRank(rows[i].Kind) < kindRank(rows[j].Kind)
		}
		return rows[i].Name < rows[j].Name
	})
	resources := splitCSV(defaultIfEmpty(resourcesValue, namespaceMatrixResources))
	if !namespaceMode {
		resources = nil
	}
	printMatrix(stdout, matrixPrintOptions{
		Cols:      cols,
		Rows:      rows,
		Mode:      matrixModeLabel(namespaceMode),
		Scope:     matrixScope(args),
		Contexts:  len(contexts),
		Failures:  countFailures(results),
		Resources: resources,
	})
	return exitFromResults(results)
}

func matrixNamespaceMode(args []string) bool {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "-n" || arg == "--namespace" || arg == "-l" || arg == "--selector" || arg == "--field-selector":
			i++
		case arg == "-A" || arg == "--all-namespaces" || arg == "--show-labels" || arg == "--ignore-not-found":
			continue
		case strings.HasPrefix(arg, "-n=") || strings.HasPrefix(arg, "--namespace="):
			continue
		case strings.HasPrefix(arg, "-l=") || strings.HasPrefix(arg, "--selector=") || strings.HasPrefix(arg, "--field-selector="):
			continue
		case strings.HasPrefix(arg, "-"):
			continue
		default:
			return false
		}
	}
	return true
}

func defaultMatrixColumns(args []string, namespaceMode bool) string {
	if namespaceMode {
		return strings.Join([]string{"context", "namespace", "kind", "name", "status", "detail", "network", "endpoints", "age"}, ",")
	}
	return strings.Join(matrixColumnsForKind(matrixTargetKind(args)), ",")
}

func matrixTargetKind(args []string) string {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "-n" || arg == "--namespace" || arg == "-l" || arg == "--selector" || arg == "--field-selector":
			i++
		case strings.HasPrefix(arg, "-"):
			continue
		default:
			return normalizeResourceKind(arg)
		}
	}
	return ""
}

func normalizeResourceKind(resource string) string {
	resource = strings.ToLower(strings.TrimSpace(resource))
	if resource == "" {
		return ""
	}
	resource = strings.SplitN(resource, "/", 2)[0]
	resource = strings.SplitN(resource, ".", 2)[0]
	switch resource {
	case "deploy", "deployment", "deployments":
		return "Deployment"
	case "sts", "statefulset", "statefulsets":
		return "StatefulSet"
	case "ds", "daemonset", "daemonsets":
		return "DaemonSet"
	case "rs", "replicaset", "replicasets":
		return "ReplicaSet"
	case "po", "pod", "pods":
		return "Pod"
	case "svc", "service", "services":
		return "Service"
	case "ing", "ingress", "ingresses":
		return "Ingress"
	case "job", "jobs":
		return "Job"
	case "cj", "cronjob", "cronjobs":
		return "CronJob"
	case "no", "node", "nodes":
		return "Node"
	case "ns", "namespace", "namespaces":
		return "Namespace"
	case "pvc", "persistentvolumeclaim", "persistentvolumeclaims":
		return "PersistentVolumeClaim"
	case "pv", "persistentvolume", "persistentvolumes":
		return "PersistentVolume"
	case "cm", "configmap", "configmaps":
		return "ConfigMap"
	case "secret", "secrets":
		return "Secret"
	default:
		return ""
	}
}

func matrixColumnsForKind(kind string) []string {
	switch kind {
	case "Service":
		return []string{"context", "namespace", "name", "type", "cluster-ip", "external-ip", "ports", "endpoints", "selector", "age"}
	case "Pod":
		return []string{"context", "namespace", "name", "status", "ready", "restarts", "pod-ip", "node", "image", "age"}
	case "Deployment", "StatefulSet", "DaemonSet", "ReplicaSet":
		return []string{"context", "namespace", "kind", "name", "status", "ready", "replicas", "image", "rollout", "age"}
	case "Ingress":
		return []string{"context", "namespace", "name", "class", "hosts", "address", "ports", "status", "age"}
	case "Job":
		return []string{"context", "namespace", "name", "status", "completions", "succeeded", "failed", "age"}
	case "CronJob":
		return []string{"context", "namespace", "name", "status", "schedule", "suspend", "active", "last-schedule", "age"}
	case "Node":
		return []string{"context", "name", "status", "roles", "internal-ip", "version", "age"}
	case "Namespace":
		return []string{"context", "name", "status", "age"}
	case "PersistentVolumeClaim":
		return []string{"context", "namespace", "name", "status", "volume", "capacity", "access-modes", "storageclass", "age"}
	case "PersistentVolume":
		return []string{"context", "name", "status", "capacity", "access-modes", "storageclass", "age"}
	case "ConfigMap", "Secret":
		return []string{"context", "namespace", "name", "data", "age"}
	default:
		return []string{"context", "namespace", "kind", "name", "status", "detail", "age"}
	}
}

func namespaceMatrixRows(kubectlPath, contextName string, args []string, resources []string, cols []string) ([]objectSummary, string, bool) {
	var rows []objectSummary
	var hardErrors []string
	for _, resource := range resources {
		obj, errOut, err := getObject(kubectlPath, contextName, append([]string{resource}, args...))
		if err != nil {
			if ignorableMatrixError(errOut) {
				continue
			}
			hardErrors = append(hardErrors, fmt.Sprintf("%s: %s", resource, oneLine(errOut)))
			continue
		}
		local := summarizeObjects(contextName, obj)
		enrichMatrixRows(kubectlPath, contextName, args, obj, local, cols)
		rows = append(rows, local...)
	}
	if len(hardErrors) > 0 {
		return rows, strings.Join(hardErrors, "\n") + "\n", true
	}
	return rows, "", false
}

func ignorableMatrixError(text string) bool {
	text = strings.ToLower(text)
	return strings.Contains(text, "the server doesn't have a resource type") ||
		strings.Contains(text, "no matches for kind")
}

func enrichMatrixRows(kubectlPath, contextName string, args []string, obj kubeAny, rows []objectSummary, cols []string) {
	if needsColumn(cols, "rollout") {
		attachRolloutStatus(kubectlPath, contextName, args, obj, rows)
	}
	if needsColumn(cols, "endpoints") {
		attachServiceEndpoints(kubectlPath, contextName, rows)
	}
}

func attachRolloutStatus(kubectlPath, contextName string, args []string, obj kubeAny, rows []objectSummary) {
	for i := range rows {
		if target := rolloutResource(objectAt(obj, i)); target != "" {
			out, _, code := runKubectlCapture(kubectlPath, contextName, append([]string{"rollout", "status", target}, append(namespaceArgs(args), "--watch=false")...), 20*time.Second)
			if code == 0 {
				rows[i].Rollout = oneLine(out)
			}
		}
	}
}

func attachServiceEndpoints(kubectlPath, contextName string, rows []objectSummary) {
	for i := range rows {
		if rows[i].Kind != "Service" || rows[i].Name == "" {
			continue
		}
		rows[i].Endpoints = serviceEndpointSummary(kubectlPath, contextName, rows[i].Namespace, rows[i].Name)
		rows[i].Detail = detailString(rows[i])
	}
}

func kindRank(kind string) int {
	switch kind {
	case "Deployment":
		return 10
	case "StatefulSet":
		return 20
	case "DaemonSet":
		return 30
	case "Pod":
		return 40
	case "Service":
		return 50
	case "Ingress":
		return 60
	case "Job":
		return 70
	case "CronJob":
		return 80
	default:
		return 999
	}
}

func matrixModeLabel(namespaceMode bool) string {
	if namespaceMode {
		return "namespace"
	}
	return "resource"
}

func matrixScope(args []string) string {
	allNamespaces := false
	namespace := ""
	var target []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "-A" || arg == "--all-namespaces":
			allNamespaces = true
		case arg == "-n" || arg == "--namespace":
			if i+1 < len(args) {
				namespace = args[i+1]
				i++
			}
		case strings.HasPrefix(arg, "-n="):
			namespace = strings.TrimPrefix(arg, "-n=")
		case strings.HasPrefix(arg, "--namespace="):
			namespace = strings.TrimPrefix(arg, "--namespace=")
		case arg == "-l" || arg == "--selector" || arg == "--field-selector":
			i++
		case strings.HasPrefix(arg, "-"):
			continue
		default:
			target = append(target, arg)
		}
	}
	scope := "current namespace"
	if allNamespaces {
		scope = "all namespaces"
	} else if namespace != "" {
		scope = "namespace/" + namespace
	}
	if len(target) == 0 {
		return scope
	}
	return strings.Join(target, " ") + " in " + scope
}

func runDiffCommand(opts globalOptions, args []string, stdout io.Writer, stderr io.Writer) int {
	return runDiffLike(opts, args, stdout, stderr, true)
}

func runDriftCommand(opts globalOptions, args []string, stdout io.Writer, stderr io.Writer) int {
	return runDiffLike(opts, args, stdout, stderr, false)
}

func runDiffLike(opts globalOptions, args []string, stdout io.Writer, stderr io.Writer, showDiff bool) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: kx [@selector] diff <resource> [-n namespace]")
		return 2
	}
	kubectlPath, contexts, err := resolveCommandContexts(opts)
	if err != nil {
		fmt.Fprintln(stderr, "kx:", err)
		return 1
	}
	type item struct {
		context string
		hash    string
		body    string
		err     string
	}
	items := make([]item, len(contexts))
	results := runContextJobs(contexts, opts, func(ctx kube.Context) history.Result {
		started := time.Now()
		obj, errOut, err := getObject(kubectlPath, ctx.Name, args)
		if err != nil {
			return history.Result{Context: ctx.Name, ExitCode: 1, Stderr: errOut, Duration: time.Since(started)}
		}
		canon := canonicalJSON(obj)
		sum := sha256.Sum256([]byte(canon))
		for i, c := range contexts {
			if c.Name == ctx.Name {
				items[i] = item{context: ctx.Name, hash: hex.EncodeToString(sum[:8]), body: canon}
			}
		}
		return history.Result{Context: ctx.Name, ExitCode: 0, Duration: time.Since(started)}
	})
	if hasFailures(results) {
		printGroupedSpecial(stderr, "diff errors", results)
		return 1
	}
	groups := map[string][]string{}
	for _, it := range items {
		groups[it.hash] = append(groups[it.hash], it.context)
	}
	fmt.Fprintf(stdout, "kx drift\n")
	fmt.Fprintf(stdout, "target: %s\n", strings.Join(args, " "))
	fmt.Fprintf(stdout, "contexts: %d  groups: %d\n\n", len(contexts), len(groups))
	hashes := make([]string, 0, len(groups))
	for hash := range groups {
		hashes = append(hashes, hash)
	}
	sort.Strings(hashes)
	printHashGroups(stdout, hashes, groups)
	if len(groups) <= 1 || !showDiff {
		return 0
	}
	base := items[0]
	for _, it := range items[1:] {
		if it.hash == base.hash {
			continue
		}
		fmt.Fprintf(stdout, "\n--- %s\n+++ %s\n", base.context, it.context)
		for _, line := range simpleDiff(base.body, it.body, 120) {
			fmt.Fprintln(stdout, line)
		}
	}
	return 1
}

func runLogsCommand(opts globalOptions, args []string, stdout io.Writer, stderr io.Writer) int {
	grep, args := removeValueFlag(args, "--grep")
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: kx [@selector] logs <resource> [-n namespace] [--grep pattern]")
		return 2
	}
	kubectlPath, contexts, err := resolveCommandContexts(opts)
	if err != nil {
		fmt.Fprintln(stderr, "kx:", err)
		return 1
	}
	kubectlArgs := append([]string{"logs"}, args...)
	if !hasFlag(kubectlArgs, "--all-containers") && !hasFlag(kubectlArgs, "--all-containers=true") {
		kubectlArgs = append(kubectlArgs, "--all-containers=true")
	}
	results := runContextJobs(contexts, opts, func(ctx kube.Context) history.Result {
		started := time.Now()
		out, errOut, code := runKubectlCapture(kubectlPath, ctx.Name, kubectlArgs, opts.Timeout)
		if grep != "" {
			out = grepLines(out, grep)
			errOut = grepLines(errOut, grep)
		}
		return history.Result{Context: ctx.Name, ExitCode: code, Stdout: out, Stderr: errOut, Duration: time.Since(started)}
	})
	printPrefixed(stdout, results)
	return exitFromResults(results)
}

func runEventsCommand(opts globalOptions, args []string, stdout io.Writer, stderr io.Writer) int {
	warnings, args := removeBoolFlag(args, "--warnings")
	sinceValue, args := removeValueFlag(args, "--since")
	limitValue, args := removeValueFlag(args, "--limit")
	limit := 40
	if limitValue != "" {
		if n, err := strconv.Atoi(limitValue); err == nil && n > 0 {
			limit = n
		}
	}
	var since time.Duration
	if sinceValue != "" {
		var err error
		since, err = time.ParseDuration(sinceValue)
		if err != nil {
			fmt.Fprintln(stderr, "kx: --since must be a Go duration like 30m or 2h")
			return 2
		}
	}
	kubectlPath, contexts, err := resolveCommandContexts(opts)
	if err != nil {
		fmt.Fprintln(stderr, "kx:", err)
		return 1
	}
	results := runContextJobs(contexts, opts, func(ctx kube.Context) history.Result {
		started := time.Now()
		rows, errText := loadEvents(kubectlPath, ctx.Name, args, warnings, since, limit)
		if errText != "" {
			return history.Result{Context: ctx.Name, ExitCode: 1, Stderr: errText, Duration: time.Since(started)}
		}
		var b strings.Builder
		if len(rows) > 0 {
			fmt.Fprintf(&b, "%-8s %-7s %-24s %-32s %s\n", "TIME", "TYPE", "REASON", "OBJECT", "MESSAGE")
			fmt.Fprintf(&b, "%-8s %-7s %-24s %-32s %s\n", "--------", "-------", "------------------------", "--------------------------------", "-------")
		}
		for _, row := range rows {
			fmt.Fprintf(&b, "%-8s %-7s %-24s %-32s %s\n", row.Time.Format("15:04:05"), row.Type, fit(row.Reason, 24), fit(row.Object, 32), row.Message)
		}
		return history.Result{Context: ctx.Name, ExitCode: 0, Stdout: b.String(), Duration: time.Since(started)}
	})
	printGroupedSpecial(stdout, "events", results)
	return exitFromResults(results)
}

func runCanCommand(opts globalOptions, args []string, stdout io.Writer, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: kx [@selector] can <verb> <resource> [-n namespace]")
		return 2
	}
	kubectlPath, contexts, err := resolveCommandContexts(opts)
	if err != nil {
		fmt.Fprintln(stderr, "kx:", err)
		return 1
	}
	kubectlArgs := append([]string{"auth", "can-i"}, args...)
	results := runContextJobs(contexts, opts, func(ctx kube.Context) history.Result {
		started := time.Now()
		out, errOut, code := runKubectlCapture(kubectlPath, ctx.Name, kubectlArgs, opts.Timeout)
		return history.Result{Context: ctx.Name, ExitCode: code, Stdout: out, Stderr: errOut, Duration: time.Since(started)}
	})
	printGroupedSpecial(stdout, "can", results)
	return exitFromResults(results)
}

func runAccessCommand(opts globalOptions, args []string, stdout io.Writer, stderr io.Writer) int {
	verbsValue, args := removeValueFlag(args, "--verbs")
	resourcesValue, args := removeValueFlag(args, "--resources")
	verbs := splitCSV(defaultIfEmpty(verbsValue, "get,list,watch,create,update,patch,delete"))
	resources := splitCSV(defaultIfEmpty(resourcesValue, "pods,deployments,services,secrets,configmaps,ingresses,nodes,namespaces"))
	nsArgs := namespaceArgs(args)
	kubectlPath, contexts, err := resolveCommandContexts(opts)
	if err != nil {
		fmt.Fprintln(stderr, "kx:", err)
		return 1
	}
	var rows []rowAccess
	var mu sync.Mutex
	results := runContextJobs(contexts, opts, func(ctx kube.Context) history.Result {
		started := time.Now()
		for _, resource := range resources {
			r := rowAccess{Context: ctx.Name, Resource: resource}
			for _, verb := range verbs {
				out, _, code := runKubectlCapture(kubectlPath, ctx.Name, append([]string{"auth", "can-i", verb, resource}, nsArgs...), opts.Timeout)
				if code == 0 && strings.TrimSpace(out) == "yes" {
					r.Values = append(r.Values, "yes")
				} else {
					r.Values = append(r.Values, "no")
				}
			}
			mu.Lock()
			rows = append(rows, r)
			mu.Unlock()
		}
		return history.Result{Context: ctx.Name, ExitCode: 0, Duration: time.Since(started)}
	})
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].Context == rows[j].Context {
			return rows[i].Resource < rows[j].Resource
		}
		return rows[i].Context < rows[j].Context
	})
	printAccess(stdout, verbs, rows)
	return exitFromResults(results)
}

func runContextScanCommand(args []string, state kube.State, meta store.Metadata, kubectlPath string, stdout io.Writer, stderr io.Writer) int {
	paths := args
	if len(paths) == 0 {
		paths = defaultKubeconfigSearchPaths()
	}
	files := discoverKubeconfigFiles(paths)
	if len(files) == 0 {
		fmt.Fprintln(stdout, "no kubeconfig files found")
		return 0
	}
	type found struct {
		Context string
		File    string
		Status  string
	}
	var foundRows []found
	for _, file := range files {
		scanned, err := kube.LoadWithPrefix(kubectlPath, []string{"--kubeconfig", file})
		if err != nil {
			fmt.Fprintf(stderr, "kx: scan skipped %s: %v\n", file, err)
			continue
		}
		for _, ctx := range scanned.Contexts {
			status := "external"
			if state.HasContext(ctx.Name) {
				status = "active"
			}
			meta.ContextSources[ctx.Name] = appendUnique(meta.ContextSources[ctx.Name], file)
			foundRows = append(foundRows, found{Context: ctx.Name, File: file, Status: status})
		}
	}
	if err := store.Save(meta); err != nil {
		fmt.Fprintln(stderr, "kx:", err)
		return 1
	}
	sort.SliceStable(foundRows, func(i, j int) bool {
		if foundRows[i].Context == foundRows[j].Context {
			return foundRows[i].File < foundRows[j].File
		}
		return foundRows[i].Context < foundRows[j].Context
	})
	fmt.Fprintf(stdout, "%-36s %-9s %s\n", "CONTEXT", "STATUS", "FILE")
	for _, row := range foundRows {
		fmt.Fprintf(stdout, "%-36s %-9s %s\n", row.Context, row.Status, row.File)
	}
	return 0
}

func runContextCleanCommand(args []string, state kube.State, meta store.Metadata, stdout io.Writer, stderr io.Writer) int {
	dryRun, _ := removeBoolFlag(args, "--dry-run")
	active := map[string]bool{}
	for _, ctx := range state.Contexts {
		active[ctx.Name] = true
	}
	var stale []string
	for name := range meta.ContextTags {
		if !active[name] {
			stale = append(stale, name)
		}
	}
	for name := range meta.ContextSources {
		if !active[name] {
			stale = appendUnique(stale, name)
		}
	}
	sort.Strings(stale)
	if len(stale) == 0 {
		fmt.Fprintln(stdout, "local kx metadata is clean")
		return 0
	}
	if dryRun {
		fmt.Fprintln(stdout, "stale local metadata:")
		for _, name := range stale {
			fmt.Fprintf(stdout, "  %s\n", name)
		}
		return 0
	}
	for _, name := range stale {
		delete(meta.ContextTags, name)
		delete(meta.ContextSources, name)
	}
	if err := store.Save(meta); err != nil {
		fmt.Fprintln(stderr, "kx:", err)
		return 1
	}
	fmt.Fprintf(stdout, "removed %d stale context metadata entrie(s)\n", len(stale))
	return 0
}

func runCompletionCommand(args []string, stdout io.Writer, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintln(stderr, "usage: kx completion bash|zsh|fish")
		return 2
	}
	switch args[0] {
	case "bash":
		fmt.Fprint(stdout, bashCompletion)
	case "zsh":
		fmt.Fprint(stdout, zshCompletion)
	case "fish":
		fmt.Fprint(stdout, fishCompletion)
	default:
		fmt.Fprintln(stderr, "usage: kx completion bash|zsh|fish")
		return 2
	}
	return 0
}

func runShellInitCommand(args []string, stdout io.Writer, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintln(stderr, "usage: kx shell-init bash|zsh|fish")
		return 2
	}
	switch args[0] {
	case "bash":
		io.WriteString(stdout, bashCompletion)
		io.WriteString(stdout, bashPrompt)
	case "zsh":
		io.WriteString(stdout, zshCompletion)
		io.WriteString(stdout, zshPrompt)
	case "fish":
		io.WriteString(stdout, fishCompletion)
		io.WriteString(stdout, fishPrompt)
	default:
		fmt.Fprintln(stderr, "usage: kx shell-init bash|zsh|fish")
		return 2
	}
	return 0
}

func runPromptCommand(args []string, stdout io.Writer, stderr io.Writer) int {
	kubectlPath, err := exec.LookPath("kubectl")
	if err != nil {
		return 0
	}
	state, err := kube.Load(kubectlPath)
	if err != nil || state.CurrentContext == "" {
		return 0
	}
	meta, _ := store.Load()
	ctx, ok := state.ContextByName(state.CurrentContext)
	if !ok {
		return 0
	}
	ns := ctx.Namespace
	if ns == "" {
		ns = "default"
	}
	tags := meta.ContextTags[ctx.Name]
	bits := []string{ctx.Name + ":" + ns}
	if tags["env"] != "" {
		bits = append(bits, "env="+tags["env"])
	}
	if tags["risk"] != "" {
		bits = append(bits, "risk="+tags["risk"])
	}
	fmt.Fprintln(stdout, strings.Join(bits, " "))
	return 0
}

func resolveCommandContexts(opts globalOptions) (string, []kube.Context, error) {
	kubectlPath, err := exec.LookPath("kubectl")
	if err != nil {
		return "", nil, errors.New("kubectl was not found in PATH")
	}
	state, err := kube.Load(kubectlPath)
	if err != nil {
		return "", nil, err
	}
	meta, err := store.Load()
	if err != nil {
		return "", nil, err
	}
	var contexts []kube.Context
	if opts.Selector == "" {
		if state.CurrentContext == "" {
			return "", nil, errors.New("no current context; use @selector")
		}
		ctx, ok := state.ContextByName(state.CurrentContext)
		if !ok {
			return "", nil, fmt.Errorf("current context %q not found in kubeconfig", state.CurrentContext)
		}
		contexts = []kube.Context{ctx}
	} else {
		contexts, err = kube.MatchSelector(opts.Selector, state, meta.ContextTags)
		if err != nil {
			return "", nil, err
		}
		if len(contexts) == 0 {
			return "", nil, fmt.Errorf("selector %q matched no contexts", opts.Selector)
		}
	}
	if opts.Canary > 0 && opts.Canary < len(contexts) {
		contexts = contexts[:opts.Canary]
	}
	return kubectlPath, contexts, nil
}

func runContextJobs(contexts []kube.Context, opts globalOptions, fn func(kube.Context) history.Result) []history.Result {
	workers := opts.Parallel
	if workers < 1 {
		workers = 1
	}
	if workers > len(contexts) {
		workers = len(contexts)
	}
	type job struct {
		index int
		ctx   kube.Context
	}
	jobs := make(chan job)
	results := make([]history.Result, len(contexts))
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				results[job.index] = fn(job.ctx)
				if results[job.index].Context == "" {
					results[job.index].Context = job.ctx.Name
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

func runKubectlCapture(kubectlPath, contextName string, args []string, timeout time.Duration) (string, string, int) {
	ctx := context.Background()
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	fullArgs := append([]string{"--context", contextName}, args...)
	cmd := exec.CommandContext(ctx, kubectlPath, fullArgs...)
	var out bytes.Buffer
	var errOut bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return out.String(), errOut.String(), exitErr.ExitCode()
		}
		if ctx.Err() == context.DeadlineExceeded {
			return out.String(), "kx: command timed out\n", 124
		}
		return out.String(), err.Error() + "\n" + errOut.String(), 1
	}
	return out.String(), errOut.String(), 0
}

func getObject(kubectlPath, contextName string, args []string) (kubeAny, string, error) {
	getArgs := append([]string{"get"}, args...)
	getArgs = append(getArgs, "-o", "json")
	out, errOut, code := runKubectlCapture(kubectlPath, contextName, getArgs, 30*time.Second)
	if code != 0 {
		return nil, errOut, errors.New("kubectl get failed")
	}
	var obj kubeAny
	if err := json.Unmarshal([]byte(out), &obj); err != nil {
		return nil, err.Error(), err
	}
	return obj, "", nil
}

func summarizeObjects(contextName string, obj kubeAny) []objectSummary {
	items, ok := obj["items"].([]any)
	if !ok {
		return []objectSummary{summarizeObject(contextName, obj)}
	}
	rows := make([]objectSummary, 0, len(items))
	for _, item := range items {
		if m, ok := item.(map[string]any); ok {
			rows = append(rows, summarizeObject(contextName, kubeAny(m)))
		}
	}
	return rows
}

func summarizeObject(contextName string, obj kubeAny) objectSummary {
	meta := mapValue(obj, "metadata")
	spec := mapValue(obj, "spec")
	status := mapValue(obj, "status")
	row := objectSummary{
		Context:   contextName,
		Kind:      stringValue(obj, "kind"),
		Name:      stringValue(meta, "name"),
		Namespace: stringValue(meta, "namespace"),
		Age:       ageString(stringValue(meta, "creationTimestamp")),
		Image:     strings.Join(imagesFromObject(obj), ","),
		Status:    statusString(obj),
		Ready:     readyString(obj),
		Warnings:  "-",
	}
	if row.Kind == "" {
		row.Kind = "Object"
	}
	row.Type = typeString(obj)
	row.ClusterIP = serviceClusterIP(spec)
	row.ExternalIP = externalAddress(obj)
	row.Ports = portsString(obj)
	row.Selector = selectorString(spec["selector"])
	row.Node = stringValue(spec, "nodeName")
	row.PodIP = stringValue(status, "podIP")
	row.Restarts = restartString(status)
	row.Class = ingressClass(obj)
	row.Hosts = ingressHosts(spec)
	row.Address = ingressAddress(obj)
	row.Schedule = stringValue(spec, "schedule")
	row.Suspend = boolString(spec["suspend"])
	row.LastSchedule = ageString(stringValue(status, "lastScheduleTime"))
	row.Completions = completionsString(spec, status)
	row.Succeeded = intish(status["succeeded"])
	row.Failed = intish(status["failed"])
	row.Active = activeString(status)
	row.Roles = nodeRoles(meta)
	row.Version = nodeVersion(status)
	row.InternalIP = nodeInternalIP(status)
	row.Capacity = capacityString(obj)
	row.AccessModes = accessModesString(spec)
	row.Volume = stringValue(spec, "volumeName")
	row.StorageClass = storageClassString(spec)
	row.Data = dataCountString(obj)
	replicas := intish(spec["replicas"])
	readyReplicas := intish(status["readyReplicas"])
	if replicas != "" || readyReplicas != "" {
		row.Replicas = readyReplicas + "/" + defaultIfEmpty(replicas, "?")
	}
	switch row.Kind {
	case "DaemonSet":
		row.Replicas = defaultIfEmpty(intish(status["numberReady"]), "0") + "/" + defaultIfEmpty(intish(status["desiredNumberScheduled"]), "?")
	case "Job":
		row.Replicas = defaultIfEmpty(intish(status["succeeded"]), "0") + "/" + defaultIfEmpty(intish(spec["completions"]), "?")
		row.Succeeded = defaultIfEmpty(row.Succeeded, "0")
		row.Failed = defaultIfEmpty(row.Failed, "0")
	case "CronJob":
		row.Active = defaultIfEmpty(row.Active, "0")
		if active := activeCount(status); active > 0 {
			row.Replicas = fmt.Sprintf("%d active", active)
		}
	}
	row.Detail = detailString(row)
	row.Network = networkString(row)
	return row
}

func objectAt(obj kubeAny, index int) kubeAny {
	items, ok := obj["items"].([]any)
	if !ok {
		return obj
	}
	if index < 0 || index >= len(items) {
		return nil
	}
	if m, ok := items[index].(map[string]any); ok {
		return kubeAny(m)
	}
	return nil
}

func mapValue(m map[string]any, key string) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	if v, ok := m[key].(map[string]any); ok {
		return v
	}
	return map[string]any{}
}

func stringValue(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	if s, ok := m[key].(string); ok {
		return s
	}
	return ""
}

func intish(v any) string {
	switch n := v.(type) {
	case float64:
		return strconv.Itoa(int(n))
	case int:
		return strconv.Itoa(n)
	case string:
		return n
	default:
		return ""
	}
}

func intValue(m map[string]any, key string) (int, bool) {
	switch n := m[key].(type) {
	case float64:
		return int(n), true
	case int:
		return n, true
	case string:
		value, err := strconv.Atoi(n)
		return value, err == nil
	default:
		return 0, false
	}
}

func typeString(obj kubeAny) string {
	kind := stringValue(obj, "kind")
	spec := mapValue(obj, "spec")
	switch kind {
	case "Service":
		return defaultIfEmpty(stringValue(spec, "type"), "ClusterIP")
	case "Secret":
		return stringValue(obj, "type")
	default:
		return ""
	}
}

func serviceClusterIP(spec map[string]any) string {
	if clusterIP := stringValue(spec, "clusterIP"); clusterIP != "" {
		return clusterIP
	}
	return ""
}

func externalAddress(obj kubeAny) string {
	kind := stringValue(obj, "kind")
	spec := mapValue(obj, "spec")
	switch kind {
	case "Service":
		if externalName := stringValue(spec, "externalName"); externalName != "" {
			return externalName
		}
		values := listStrings(spec["externalIPs"])
		values = append(values, loadBalancerAddresses(mapValue(obj, "status"))...)
		return strings.Join(uniqueStrings(values), ",")
	case "Ingress":
		return ingressAddress(obj)
	default:
		return ""
	}
}

func loadBalancerAddresses(status map[string]any) []string {
	lb := mapValue(status, "loadBalancer")
	ingress, ok := lb["ingress"].([]any)
	if !ok {
		return nil
	}
	var values []string
	for _, item := range ingress {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if ip := stringValue(m, "ip"); ip != "" {
			values = append(values, ip)
		}
		if hostname := stringValue(m, "hostname"); hostname != "" {
			values = append(values, hostname)
		}
	}
	return values
}

func portsString(obj kubeAny) string {
	kind := stringValue(obj, "kind")
	spec := mapValue(obj, "spec")
	switch kind {
	case "Service":
		ports, ok := spec["ports"].([]any)
		if !ok {
			return ""
		}
		var values []string
		for _, item := range ports {
			port, ok := item.(map[string]any)
			if !ok {
				continue
			}
			value := intish(port["port"])
			if targetPort := intish(port["targetPort"]); targetPort != "" && targetPort != value {
				value += "->" + targetPort
			}
			if nodePort := intish(port["nodePort"]); nodePort != "" {
				value += ":" + nodePort
			}
			if protocol := stringValue(port, "protocol"); protocol != "" {
				value += "/" + protocol
			}
			if name := stringValue(port, "name"); name != "" {
				value = name + "=" + value
			}
			values = append(values, value)
		}
		return strings.Join(values, ",")
	case "Ingress":
		if tls, ok := spec["tls"].([]any); ok && len(tls) > 0 {
			return "80,443"
		}
		return "80"
	default:
		return ""
	}
}

func selectorString(value any) string {
	selector, ok := value.(map[string]any)
	if !ok || len(selector) == 0 {
		return ""
	}
	keys := make([]string, 0, len(selector))
	for key := range selector {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	values := make([]string, 0, len(keys))
	for _, key := range keys {
		values = append(values, key+"="+fmt.Sprint(selector[key]))
	}
	return strings.Join(values, ",")
}

func restartString(status map[string]any) string {
	restarts := 0
	seen := false
	for _, key := range []string{"initContainerStatuses", "containerStatuses"} {
		statuses, ok := status[key].([]any)
		if !ok {
			continue
		}
		for _, item := range statuses {
			container, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if count, ok := intValue(container, "restartCount"); ok {
				restarts += count
				seen = true
			}
		}
	}
	if !seen {
		return ""
	}
	return strconv.Itoa(restarts)
}

func ingressClass(obj kubeAny) string {
	spec := mapValue(obj, "spec")
	if className := stringValue(spec, "ingressClassName"); className != "" {
		return className
	}
	meta := mapValue(obj, "metadata")
	annotations := mapValue(meta, "annotations")
	return stringValue(annotations, "kubernetes.io/ingress.class")
}

func ingressHosts(spec map[string]any) string {
	rules, ok := spec["rules"].([]any)
	if !ok || len(rules) == 0 {
		return ""
	}
	var hosts []string
	for _, item := range rules {
		rule, ok := item.(map[string]any)
		if !ok {
			continue
		}
		hosts = append(hosts, defaultIfEmpty(stringValue(rule, "host"), "*"))
	}
	return strings.Join(uniqueStrings(hosts), ",")
}

func ingressAddress(obj kubeAny) string {
	return strings.Join(loadBalancerAddresses(mapValue(obj, "status")), ",")
}

func boolString(value any) string {
	if b, ok := value.(bool); ok {
		if b {
			return "true"
		}
		return "false"
	}
	return ""
}

func completionsString(spec map[string]any, status map[string]any) string {
	succeededValue := intish(status["succeeded"])
	completionsValue := intish(spec["completions"])
	if succeededValue == "" && completionsValue == "" {
		return ""
	}
	succeeded := defaultIfEmpty(succeededValue, "0")
	completions := defaultIfEmpty(completionsValue, "?")
	return succeeded + "/" + completions
}

func activeString(status map[string]any) string {
	if active, ok := intValue(status, "active"); ok {
		return strconv.Itoa(active)
	}
	if active := activeCount(status); active > 0 {
		return strconv.Itoa(active)
	}
	return ""
}

func nodeRoles(meta map[string]any) string {
	labels := mapValue(meta, "labels")
	var roles []string
	for key, value := range labels {
		if strings.HasPrefix(key, "node-role.kubernetes.io/") {
			role := strings.TrimPrefix(key, "node-role.kubernetes.io/")
			if role == "" {
				role = fmt.Sprint(value)
			}
			roles = append(roles, role)
		}
	}
	if role := stringValue(labels, "kubernetes.io/role"); role != "" {
		roles = append(roles, role)
	}
	if len(roles) == 0 {
		return "<none>"
	}
	sort.Strings(roles)
	return strings.Join(uniqueStrings(roles), ",")
}

func nodeVersion(status map[string]any) string {
	nodeInfo := mapValue(status, "nodeInfo")
	return stringValue(nodeInfo, "kubeletVersion")
}

func nodeInternalIP(status map[string]any) string {
	addresses, ok := status["addresses"].([]any)
	if !ok {
		return ""
	}
	for _, item := range addresses {
		address, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if stringValue(address, "type") == "InternalIP" {
			return stringValue(address, "address")
		}
	}
	return ""
}

func capacityString(obj kubeAny) string {
	status := mapValue(obj, "status")
	if capacity := mapValue(status, "capacity"); len(capacity) > 0 {
		if storage := stringValue(capacity, "storage"); storage != "" {
			return storage
		}
	}
	spec := mapValue(obj, "spec")
	if capacity := mapValue(spec, "capacity"); len(capacity) > 0 {
		return stringValue(capacity, "storage")
	}
	return ""
}

func accessModesString(spec map[string]any) string {
	return strings.Join(listStrings(spec["accessModes"]), ",")
}

func storageClassString(spec map[string]any) string {
	return stringValue(spec, "storageClassName")
}

func dataCountString(obj kubeAny) string {
	count := 0
	if data, ok := obj["data"].(map[string]any); ok {
		count += len(data)
	}
	if data, ok := obj["binaryData"].(map[string]any); ok {
		count += len(data)
	}
	if count == 0 {
		return ""
	}
	return strconv.Itoa(count)
}

func detailString(row objectSummary) string {
	switch row.Kind {
	case "Service":
		return strings.Join(nonEmpty(row.Type, row.ClusterIP, row.Ports), " ")
	case "Pod":
		return strings.Join(nonEmpty("ready="+dash(row.Ready), "restarts="+dash(row.Restarts)), " ")
	case "Deployment", "StatefulSet", "DaemonSet", "ReplicaSet":
		return strings.Join(nonEmpty("ready="+dash(row.Ready), "replicas="+dash(row.Replicas)), " ")
	case "Ingress":
		return strings.Join(nonEmpty("class="+dash(row.Class), "hosts="+dash(row.Hosts)), " ")
	case "Job":
		return strings.Join(nonEmpty("complete="+dash(row.Completions), "failed="+dash(row.Failed)), " ")
	case "CronJob":
		return strings.Join(nonEmpty(row.Schedule, "suspend="+dash(row.Suspend)), " ")
	case "Node":
		return strings.Join(nonEmpty(row.Roles, row.Version), " ")
	case "PersistentVolumeClaim", "PersistentVolume":
		return strings.Join(nonEmpty(row.Capacity, row.AccessModes, row.StorageClass), " ")
	case "ConfigMap", "Secret":
		if row.Data == "" {
			return ""
		}
		return row.Data + " keys"
	default:
		return strings.Join(nonEmpty(row.Ready, row.Replicas, row.Type), " ")
	}
}

func networkString(row objectSummary) string {
	switch row.Kind {
	case "Service":
		return defaultIfEmpty(row.ExternalIP, row.ClusterIP)
	case "Pod":
		return strings.Join(nonEmpty(row.PodIP, row.Node), " ")
	case "Ingress":
		return row.Address
	case "Node":
		return row.InternalIP
	default:
		return ""
	}
}

func nonEmpty(values ...string) []string {
	var out []string
	for _, value := range values {
		if strings.TrimSpace(value) != "" && value != "-" {
			out = append(out, value)
		}
	}
	return out
}

func listStrings(value any) []string {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	var values []string
	for _, item := range items {
		if s, ok := item.(string); ok && s != "" {
			values = append(values, s)
		}
	}
	return values
}

func imagesFromObject(obj kubeAny) []string {
	spec := mapValue(obj, "spec")
	podSpec := spec
	if template := mapValue(spec, "template"); len(template) > 0 {
		podSpec = mapValue(template, "spec")
	}
	var images []string
	addImages := func(value any) {
		containers, ok := value.([]any)
		if !ok {
			return
		}
		for _, c := range containers {
			if m, ok := c.(map[string]any); ok {
				if img := stringValue(m, "image"); img != "" {
					images = append(images, img)
				}
			}
		}
	}
	addImages(podSpec["containers"])
	addImages(podSpec["initContainers"])
	return uniqueStrings(images)
}

func readyString(obj kubeAny) string {
	kind := stringValue(obj, "kind")
	status := mapValue(obj, "status")
	spec := mapValue(obj, "spec")
	switch kind {
	case "Pod":
		phase := stringValue(status, "phase")
		ready := 0
		total := 0
		if statuses, ok := status["containerStatuses"].([]any); ok {
			for _, s := range statuses {
				if m, ok := s.(map[string]any); ok {
					total++
					if readyValue, ok := m["ready"].(bool); ok && readyValue {
						ready++
					}
				}
			}
		}
		if total > 0 {
			return fmt.Sprintf("%s %d/%d", defaultIfEmpty(phase, "?"), ready, total)
		}
		return phase
	case "Deployment", "StatefulSet", "ReplicaSet":
		return defaultIfEmpty(intish(status["readyReplicas"]), "0") + "/" + defaultIfEmpty(intish(spec["replicas"]), "?")
	case "DaemonSet":
		return defaultIfEmpty(intish(status["numberReady"]), "0") + "/" + defaultIfEmpty(intish(status["desiredNumberScheduled"]), "?")
	default:
		if phase := stringValue(status, "phase"); phase != "" {
			return phase
		}
		for _, cond := range objectConditions(obj) {
			if strings.Contains(cond, "Ready=") {
				return cond
			}
		}
	}
	return "-"
}

func statusString(obj kubeAny) string {
	kind := stringValue(obj, "kind")
	meta := mapValue(obj, "metadata")
	spec := mapValue(obj, "spec")
	status := mapValue(obj, "status")
	if stringValue(meta, "deletionTimestamp") != "" {
		return "Terminating"
	}
	switch kind {
	case "Pod":
		if reason := containerStateReason(status); reason != "" {
			return reason
		}
		phase := defaultIfEmpty(stringValue(status, "phase"), "Unknown")
		if phase == "Running" && readyMismatch(readyString(obj)) {
			return "NotReady"
		}
		return phase
	case "Deployment", "StatefulSet", "ReplicaSet":
		desired, desiredOK := intValue(spec, "replicas")
		ready, readyOK := intValue(status, "readyReplicas")
		if unavailable, ok := intValue(status, "unavailableReplicas"); ok && unavailable > 0 {
			return "Degraded"
		}
		if desiredOK && desired == 0 {
			return "ScaledToZero"
		}
		if desiredOK && readyOK && ready >= desired {
			return "Ready"
		}
		return "Progressing"
	case "DaemonSet":
		desired, desiredOK := intValue(status, "desiredNumberScheduled")
		ready, readyOK := intValue(status, "numberReady")
		if desiredOK && desired == 0 {
			return "ScaledToZero"
		}
		if desiredOK && readyOK && ready >= desired {
			return "Ready"
		}
		return "Degraded"
	case "Job":
		if hasCondition(status, "Failed", "True") {
			return "Failed"
		}
		if hasCondition(status, "Complete", "True") {
			return "Complete"
		}
		if active, ok := intValue(status, "active"); ok && active > 0 {
			return "Running"
		}
		return "Pending"
	case "CronJob":
		if suspended, ok := spec["suspend"].(bool); ok && suspended {
			return "Suspended"
		}
		if active := activeCount(status); active > 0 {
			return fmt.Sprintf("Active:%d", active)
		}
		return "Idle"
	case "Service":
		serviceType := defaultIfEmpty(stringValue(spec, "type"), "ClusterIP")
		if serviceType == "LoadBalancer" {
			if externalAddress(obj) != "" {
				return "Exposed"
			}
			return "PendingLB"
		}
		return serviceType
	case "Ingress":
		lb := mapValue(status, "loadBalancer")
		if ingress, ok := lb["ingress"].([]any); ok && len(ingress) > 0 {
			return "Ready"
		}
		return "Pending"
	default:
		if phase := stringValue(status, "phase"); phase != "" {
			return phase
		}
		if hasCondition(status, "Ready", "True") {
			return "Ready"
		}
		if hasCondition(status, "Ready", "False") {
			return "NotReady"
		}
	}
	return "-"
}

func containerStateReason(status map[string]any) string {
	for _, key := range []string{"initContainerStatuses", "containerStatuses"} {
		statuses, ok := status[key].([]any)
		if !ok {
			continue
		}
		for _, item := range statuses {
			container, ok := item.(map[string]any)
			if !ok {
				continue
			}
			state := mapValue(container, "state")
			waiting := mapValue(state, "waiting")
			if reason := stringValue(waiting, "reason"); reason != "" {
				return reason
			}
			terminated := mapValue(state, "terminated")
			if reason := stringValue(terminated, "reason"); reason != "" && reason != "Completed" {
				return reason
			}
		}
	}
	return ""
}

func hasCondition(status map[string]any, condType string, condStatus string) bool {
	conditions, ok := status["conditions"].([]any)
	if !ok {
		return false
	}
	for _, item := range conditions {
		cond, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if stringValue(cond, "type") == condType && stringValue(cond, "status") == condStatus {
			return true
		}
	}
	return false
}

func activeCount(status map[string]any) int {
	active, ok := status["active"].([]any)
	if !ok {
		return 0
	}
	return len(active)
}

func readyMismatch(value string) bool {
	fields := strings.Fields(value)
	if len(fields) == 0 {
		return false
	}
	parts := strings.Split(fields[len(fields)-1], "/")
	if len(parts) != 2 {
		return false
	}
	return parts[0] != parts[1]
}

func objectConditions(obj kubeAny) []string {
	status := mapValue(obj, "status")
	conditions, ok := status["conditions"].([]any)
	if !ok {
		return nil
	}
	var lines []string
	for _, item := range conditions {
		cond, ok := item.(map[string]any)
		if !ok {
			continue
		}
		line := fmt.Sprintf("%s=%s", stringValue(cond, "type"), stringValue(cond, "status"))
		if reason := stringValue(cond, "reason"); reason != "" {
			line += " reason=" + reason
		}
		if msg := stringValue(cond, "message"); msg != "" {
			line += " msg=" + strings.TrimSpace(msg)
		}
		lines = append(lines, line)
	}
	return lines
}

func ownerChain(obj kubeAny) string {
	meta := mapValue(obj, "metadata")
	owners, ok := meta["ownerReferences"].([]any)
	if !ok {
		return ""
	}
	var out []string
	for _, owner := range owners {
		if m, ok := owner.(map[string]any); ok {
			out = append(out, stringValue(m, "kind")+"/"+stringValue(m, "name"))
		}
	}
	return strings.Join(out, " <- ")
}

func rolloutResource(obj kubeAny) string {
	if obj == nil {
		return ""
	}
	kind := stringValue(obj, "kind")
	name := stringValue(mapValue(obj, "metadata"), "name")
	switch kind {
	case "Deployment":
		return "deployment/" + name
	case "StatefulSet":
		return "statefulset/" + name
	case "DaemonSet":
		return "daemonset/" + name
	default:
		return ""
	}
}

func warningEvents(kubectlPath, contextName string, obj kubeAny, nsArgs []string) string {
	meta := mapValue(obj, "metadata")
	name := stringValue(meta, "name")
	if name == "" {
		return ""
	}
	args := append([]string{"get", "events", "--field-selector", "type=Warning,involvedObject.name=" + name, "--sort-by=.lastTimestamp"}, nsArgs...)
	out, _, code := runKubectlCapture(kubectlPath, contextName, args, 20*time.Second)
	if code != 0 {
		return ""
	}
	return out
}

func serviceEndpointSummary(kubectlPath, contextName string, namespace string, serviceName string) string {
	args := append([]string{"get", "endpointslices.discovery.k8s.io", "-l", "kubernetes.io/service-name=" + serviceName, "-o", "json"}, endpointNamespaceArgs(namespace)...)
	out, _, code := runKubectlCapture(kubectlPath, contextName, args, 15*time.Second)
	if code == 0 {
		var root kubeAny
		if err := json.Unmarshal([]byte(out), &root); err == nil {
			ready, total := endpointSummaryFromSlices(root)
			return formatEndpointSummary(ready, total)
		}
	}

	args = append([]string{"get", "endpoints", serviceName, "-o", "json"}, endpointNamespaceArgs(namespace)...)
	out, _, code = runKubectlCapture(kubectlPath, contextName, args, 15*time.Second)
	if code != 0 {
		return "-"
	}
	var root kubeAny
	if err := json.Unmarshal([]byte(out), &root); err != nil {
		return "-"
	}
	ready, total := endpointSummaryFromEndpoints(root)
	return formatEndpointSummary(ready, total)
}

func endpointNamespaceArgs(namespace string) []string {
	if namespace == "" {
		return nil
	}
	return []string{"-n", namespace}
}

func endpointSummaryFromSlices(root kubeAny) (int, int) {
	items, ok := root["items"].([]any)
	if !ok {
		return 0, 0
	}
	ready := 0
	total := 0
	for _, item := range items {
		slice, ok := item.(map[string]any)
		if !ok {
			continue
		}
		endpoints, ok := slice["endpoints"].([]any)
		if !ok {
			continue
		}
		for _, item := range endpoints {
			endpoint, ok := item.(map[string]any)
			if !ok {
				continue
			}
			addresses := listStrings(endpoint["addresses"])
			count := len(addresses)
			if count == 0 {
				count = 1
			}
			total += count
			conditions := mapValue(endpoint, "conditions")
			if readyValue, ok := conditions["ready"].(bool); ok && !readyValue {
				continue
			}
			ready += count
		}
	}
	return ready, total
}

func endpointSummaryFromEndpoints(root kubeAny) (int, int) {
	subsets, ok := root["subsets"].([]any)
	if !ok {
		return 0, 0
	}
	ready := 0
	total := 0
	for _, item := range subsets {
		subset, ok := item.(map[string]any)
		if !ok {
			continue
		}
		readyAddresses, _ := subset["addresses"].([]any)
		notReadyAddresses, _ := subset["notReadyAddresses"].([]any)
		ready += len(readyAddresses)
		total += len(readyAddresses) + len(notReadyAddresses)
	}
	return ready, total
}

func formatEndpointSummary(ready int, total int) string {
	if total == 0 {
		return "0/0"
	}
	return fmt.Sprintf("%d/%d", ready, total)
}

func loadEvents(kubectlPath, contextName string, args []string, warnings bool, since time.Duration, limit int) ([]eventRow, string) {
	kubectlArgs := append([]string{"get", "events", "-o", "json"}, args...)
	if warnings {
		kubectlArgs = append(kubectlArgs, "--field-selector", "type=Warning")
	}
	out, errOut, code := runKubectlCapture(kubectlPath, contextName, kubectlArgs, 30*time.Second)
	if code != 0 {
		return nil, errOut
	}
	var root kubeAny
	if err := json.Unmarshal([]byte(out), &root); err != nil {
		return nil, err.Error()
	}
	items, _ := root["items"].([]any)
	cutoff := time.Time{}
	if since > 0 {
		cutoff = time.Now().Add(-since)
	}
	var rows []eventRow
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		t := eventTime(m)
		if !cutoff.IsZero() && t.Before(cutoff) {
			continue
		}
		involved := mapValue(m, "involvedObject")
		obj := stringValue(involved, "kind") + "/" + stringValue(involved, "name")
		rows = append(rows, eventRow{
			Time:      t,
			Type:      stringValue(m, "type"),
			Reason:    stringValue(m, "reason"),
			Object:    obj,
			Namespace: stringValue(involved, "namespace"),
			Message:   compact(stringValue(m, "message"), 90),
			Count:     intish(m["count"]),
		})
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].Time.Before(rows[j].Time) })
	if limit > 0 && len(rows) > limit {
		rows = rows[len(rows)-limit:]
	}
	return rows, ""
}

func eventTime(m map[string]any) time.Time {
	keys := []string{"eventTime", "lastTimestamp", "firstTimestamp"}
	for _, key := range keys {
		if t, ok := parseTime(stringValue(m, key)); ok {
			return t
		}
	}
	meta := mapValue(m, "metadata")
	if t, ok := parseTime(stringValue(meta, "creationTimestamp")); ok {
		return t
	}
	return time.Time{}
}

func parseTime(value string) (time.Time, bool) {
	if value == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, value)
	return t, err == nil
}

func canonicalJSON(obj kubeAny) string {
	clean := sanitizeJSON(obj)
	data, _ := json.MarshalIndent(clean, "", "  ")
	return string(data)
}

func sanitizeJSON(v any) any {
	switch x := v.(type) {
	case kubeAny:
		return sanitizeJSON(map[string]any(x))
	case map[string]any:
		out := map[string]any{}
		for k, v := range x {
			if k == "status" {
				continue
			}
			if k == "metadata" {
				if meta, ok := v.(map[string]any); ok {
					out[k] = sanitizeMetadata(meta)
				}
				continue
			}
			out[k] = sanitizeJSON(v)
		}
		return out
	case []any:
		out := make([]any, 0, len(x))
		for _, item := range x {
			out = append(out, sanitizeJSON(item))
		}
		return out
	default:
		return v
	}
}

func sanitizeMetadata(meta map[string]any) map[string]any {
	drop := map[string]bool{
		"creationTimestamp": true,
		"generation":        true,
		"managedFields":     true,
		"resourceVersion":   true,
		"uid":               true,
	}
	out := map[string]any{}
	for k, v := range meta {
		if drop[k] {
			continue
		}
		out[k] = sanitizeJSON(v)
	}
	return out
}

type matrixPrintOptions struct {
	Cols      []string
	Rows      []objectSummary
	Mode      string
	Scope     string
	Contexts  int
	Failures  int
	Resources []string
}

func printMatrix(w io.Writer, opts matrixPrintOptions) {
	cols := opts.Cols
	rows := opts.Rows
	fmt.Fprintln(w, "kx matrix")
	fmt.Fprintf(w, "mode: %s  scope: %s  contexts: %d  rows: %d  failures: %d\n", opts.Mode, opts.Scope, opts.Contexts, len(rows), opts.Failures)
	if len(opts.Resources) > 0 {
		fmt.Fprintf(w, "resources: %s\n", strings.Join(opts.Resources, ","))
	}
	fmt.Fprintln(w)
	if len(rows) == 0 {
		fmt.Fprintln(w, "no rows")
		return
	}
	widths := map[string]int{}
	for _, col := range cols {
		widths[col] = len(strings.ToUpper(col))
	}
	for _, row := range rows {
		values := summaryValues(row)
		for _, col := range cols {
			value := fit(dash(values[col]), matrixColumnLimit(col))
			if len(value) > widths[col] {
				widths[col] = len(value)
			}
		}
	}
	printTableLine(w, cols, widths, func(col string) string { return strings.ToUpper(col) })
	printTableLine(w, cols, widths, func(col string) string { return strings.Repeat("-", widths[col]) })
	for _, row := range rows {
		values := summaryValues(row)
		printTableLine(w, cols, widths, func(col string) string {
			return fit(dash(values[col]), matrixColumnLimit(col))
		})
	}
}

func printTableLine(w io.Writer, cols []string, widths map[string]int, value func(string) string) {
	for i, col := range cols {
		if i > 0 {
			fmt.Fprint(w, "  ")
		}
		fmt.Fprintf(w, "%-*s", widths[col], value(col))
	}
	fmt.Fprintln(w)
}

func matrixColumnLimit(col string) int {
	switch col {
	case "context":
		return 34
	case "namespace":
		return 24
	case "kind":
		return 16
	case "name":
		return 48
	case "status":
		return 28
	case "ready":
		return 28
	case "image":
		return 64
	case "type":
		return 18
	case "cluster-ip", "external-ip", "pod-ip", "internal-ip", "address", "network":
		return 40
	case "ports":
		return 42
	case "selector":
		return 56
	case "endpoints":
		return 12
	case "node", "class", "roles", "version", "volume", "storageclass":
		return 34
	case "hosts":
		return 56
	case "schedule":
		return 32
	case "last-schedule", "access-modes", "capacity", "completions", "succeeded", "failed", "active", "suspend", "restarts", "data":
		return 18
	case "detail":
		return 64
	case "rollout":
		return 72
	case "warnings":
		return 72
	case "hash":
		return 16
	default:
		return 40
	}
}

func summaryValues(row objectSummary) map[string]string {
	return map[string]string{
		"context":       row.Context,
		"kind":          row.Kind,
		"name":          row.Name,
		"namespace":     row.Namespace,
		"status":        row.Status,
		"ready":         row.Ready,
		"image":         row.Image,
		"replicas":      row.Replicas,
		"rollout":       row.Rollout,
		"age":           row.Age,
		"warnings":      row.Warnings,
		"hash":          row.Hash,
		"type":          row.Type,
		"cluster-ip":    row.ClusterIP,
		"external-ip":   row.ExternalIP,
		"ports":         row.Ports,
		"selector":      row.Selector,
		"endpoints":     row.Endpoints,
		"node":          row.Node,
		"pod-ip":        row.PodIP,
		"restarts":      row.Restarts,
		"class":         row.Class,
		"hosts":         row.Hosts,
		"address":       row.Address,
		"schedule":      row.Schedule,
		"suspend":       row.Suspend,
		"last-schedule": row.LastSchedule,
		"completions":   row.Completions,
		"succeeded":     row.Succeeded,
		"failed":        row.Failed,
		"active":        row.Active,
		"roles":         row.Roles,
		"version":       row.Version,
		"internal-ip":   row.InternalIP,
		"capacity":      row.Capacity,
		"access-modes":  row.AccessModes,
		"volume":        row.Volume,
		"storageclass":  row.StorageClass,
		"data":          row.Data,
		"detail":        row.Detail,
		"network":       row.Network,
	}
}

func summaryFacts(row objectSummary) [][2]string {
	switch row.Kind {
	case "Service":
		return factPairs(
			"type", row.Type,
			"status", row.Status,
			"cluster-ip", row.ClusterIP,
			"external-ip", row.ExternalIP,
			"ports", row.Ports,
			"selector", row.Selector,
			"endpoints", row.Endpoints,
			"age", row.Age,
		)
	case "Pod":
		return factPairs(
			"status", row.Status,
			"ready", row.Ready,
			"restarts", row.Restarts,
			"pod-ip", row.PodIP,
			"node", row.Node,
			"image", row.Image,
			"age", row.Age,
		)
	case "Deployment", "StatefulSet", "DaemonSet", "ReplicaSet":
		return factPairs(
			"status", row.Status,
			"ready", row.Ready,
			"replicas", row.Replicas,
			"image", row.Image,
			"age", row.Age,
		)
	case "Ingress":
		return factPairs(
			"status", row.Status,
			"class", row.Class,
			"hosts", row.Hosts,
			"address", row.Address,
			"ports", row.Ports,
			"age", row.Age,
		)
	case "Job":
		return factPairs(
			"status", row.Status,
			"completions", row.Completions,
			"succeeded", row.Succeeded,
			"failed", row.Failed,
			"age", row.Age,
		)
	case "CronJob":
		return factPairs(
			"status", row.Status,
			"schedule", row.Schedule,
			"suspend", row.Suspend,
			"active", row.Active,
			"last-schedule", row.LastSchedule,
			"age", row.Age,
		)
	case "Node":
		return factPairs(
			"status", row.Status,
			"roles", row.Roles,
			"internal-ip", row.InternalIP,
			"version", row.Version,
			"age", row.Age,
		)
	case "PersistentVolumeClaim", "PersistentVolume":
		return factPairs(
			"status", row.Status,
			"volume", row.Volume,
			"capacity", row.Capacity,
			"access-modes", row.AccessModes,
			"storageclass", row.StorageClass,
			"age", row.Age,
		)
	case "ConfigMap", "Secret":
		return factPairs("data", row.Data, "age", row.Age)
	default:
		return factPairs(
			"status", row.Status,
			"ready", row.Ready,
			"detail", row.Detail,
			"age", row.Age,
		)
	}
}

func factPairs(values ...string) [][2]string {
	var facts [][2]string
	for i := 0; i+1 < len(values); i += 2 {
		if strings.TrimSpace(values[i+1]) == "" {
			continue
		}
		facts = append(facts, [2]string{values[i], values[i+1]})
	}
	return facts
}

func printGroupedSpecial(w io.Writer, label string, results []history.Result) {
	errorsOnly := strings.Contains(label, "errors")
	failures := countFailures(results)
	ok := len(results) - failures
	fmt.Fprintf(w, "kx %s\n", label)
	fmt.Fprintf(w, "contexts: %d  ok: %d  failed: %d\n\n", len(results), ok, failures)
	for _, result := range results {
		if errorsOnly && result.ExitCode == 0 && result.Stderr == "" {
			continue
		}
		status := "ok"
		if result.ExitCode != 0 {
			status = fmt.Sprintf("exit %d", result.ExitCode)
		}
		fmt.Fprintf(w, "[%s] %s  %s\n", result.Context, status, durationLabel(result.Duration))
		fmt.Fprintln(w, strings.Repeat("-", 48))
		wrote := false
		if result.Stdout != "" {
			fmt.Fprint(w, result.Stdout)
			if !strings.HasSuffix(result.Stdout, "\n") {
				fmt.Fprintln(w)
			}
			wrote = true
		}
		if result.Stderr != "" {
			fmt.Fprint(w, result.Stderr)
			if !strings.HasSuffix(result.Stderr, "\n") {
				fmt.Fprintln(w)
			}
			wrote = true
		}
		if !wrote {
			fmt.Fprintln(w, "(no output)")
		}
		fmt.Fprintln(w)
	}
}

func printPrefixed(w io.Writer, results []history.Result) {
	width := len("context")
	for _, result := range results {
		if len(result.Context) > width {
			width = len(result.Context)
		}
	}
	for _, result := range results {
		for _, line := range splitOutputLines(result.Stdout) {
			fmt.Fprintf(w, "%-*s | out | %s\n", width, result.Context, line)
		}
		for _, line := range splitOutputLines(result.Stderr) {
			fmt.Fprintf(w, "%-*s | err | %s\n", width, result.Context, line)
		}
	}
}

func countFailures(results []history.Result) int {
	failures := 0
	for _, result := range results {
		if result.ExitCode != 0 {
			failures++
		}
	}
	return failures
}

func durationLabel(d time.Duration) string {
	if d <= 0 {
		return "-"
	}
	if d < time.Second {
		return d.Round(time.Millisecond).String()
	}
	return d.Round(100 * time.Millisecond).String()
}

func printHashGroups(w io.Writer, hashes []string, groups map[string][]string) {
	widthHash := len("HASH")
	for _, hash := range hashes {
		if len(hash) > widthHash {
			widthHash = len(hash)
		}
	}
	fmt.Fprintf(w, "%-*s  %-5s  %s\n", widthHash, "HASH", "COUNT", "CONTEXTS")
	fmt.Fprintf(w, "%-*s  %-5s  %s\n", widthHash, strings.Repeat("-", widthHash), "-----", "--------")
	for _, hash := range hashes {
		contexts := append([]string(nil), groups[hash]...)
		sort.Strings(contexts)
		fmt.Fprintf(w, "%-*s  %-5d  %s\n", widthHash, hash, len(contexts), strings.Join(contexts, ","))
	}
}

func exitFromResults(results []history.Result) int {
	for _, result := range results {
		if result.ExitCode != 0 {
			return 1
		}
	}
	return 0
}

func simpleDiff(a, b string, limit int) []string {
	left := strings.Split(a, "\n")
	right := strings.Split(b, "\n")
	max := len(left)
	if len(right) > max {
		max = len(right)
	}
	var out []string
	for i := 0; i < max && len(out) < limit; i++ {
		var l, r string
		if i < len(left) {
			l = left[i]
		}
		if i < len(right) {
			r = right[i]
		}
		if l == r {
			continue
		}
		if l != "" {
			out = append(out, "-"+l)
		}
		if r != "" {
			out = append(out, "+"+r)
		}
	}
	if len(out) == limit {
		out = append(out, "... diff truncated")
	}
	return out
}

func printAccess(w io.Writer, verbs []string, rows []rowAccess) {
	widthContext := len("CONTEXT")
	widthResource := len("RESOURCE")
	for _, row := range rows {
		if len(row.Context) > widthContext {
			widthContext = len(row.Context)
		}
		if len(row.Resource) > widthResource {
			widthResource = len(row.Resource)
		}
	}
	fmt.Fprintf(w, "%-*s  %-*s", widthContext, "CONTEXT", widthResource, "RESOURCE")
	for _, verb := range verbs {
		fmt.Fprintf(w, "  %-6s", strings.ToUpper(verb))
	}
	fmt.Fprintln(w)
	for _, row := range rows {
		fmt.Fprintf(w, "%-*s  %-*s", widthContext, row.Context, widthResource, row.Resource)
		for _, value := range row.Values {
			cell := "no"
			if value == "yes" {
				cell = "yes"
			}
			fmt.Fprintf(w, "  %-6s", cell)
		}
		fmt.Fprintln(w)
	}
}

type rowAccess struct {
	Context  string
	Resource string
	Values   []string
}

func namespaceArgs(args []string) []string {
	for i, arg := range args {
		switch {
		case arg == "-A" || arg == "--all-namespaces":
			return []string{arg}
		case arg == "-n" || arg == "--namespace":
			if i+1 < len(args) {
				return []string{arg, args[i+1]}
			}
		case strings.HasPrefix(arg, "-n=") || strings.HasPrefix(arg, "--namespace="):
			return []string{arg}
		}
	}
	return nil
}

func removeBoolFlag(args []string, name string) (bool, []string) {
	out := make([]string, 0, len(args))
	found := false
	for _, arg := range args {
		if arg == name {
			found = true
			continue
		}
		out = append(out, arg)
	}
	return found, out
}

func removeValueFlag(args []string, name string) (string, []string) {
	out := make([]string, 0, len(args))
	value := ""
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == name {
			if i+1 < len(args) {
				value = args[i+1]
				i++
			}
			continue
		}
		if strings.HasPrefix(arg, name+"=") {
			value = strings.TrimPrefix(arg, name+"=")
			continue
		}
		out = append(out, arg)
	}
	return value, out
}

func hasFlag(args []string, name string) bool {
	for _, arg := range args {
		if arg == name || strings.HasPrefix(arg, name+"=") {
			return true
		}
	}
	return false
}

func grepLines(text, pattern string) string {
	if pattern == "" {
		return text
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		re = regexp.MustCompile(regexp.QuoteMeta(pattern))
	}
	var out []string
	for _, line := range splitOutputLines(text) {
		if re.MatchString(line) {
			out = append(out, line)
		}
	}
	if len(out) == 0 {
		return ""
	}
	return strings.Join(out, "\n") + "\n"
}

func splitOutputLines(text string) []string {
	text = strings.TrimSuffix(text, "\n")
	if text == "" {
		return nil
	}
	return strings.Split(text, "\n")
}

func tailLines(text string, n int) []string {
	lines := splitOutputLines(text)
	if n > 0 && len(lines) > n {
		return lines[len(lines)-n:]
	}
	return lines
}

func ageString(value string) string {
	t, ok := parseTime(value)
	if !ok {
		return ""
	}
	d := time.Since(t)
	switch {
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}

func splitCSV(value string) []string {
	var out []string
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(strings.ToLower(part))
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func needsColumn(cols []string, want string) bool {
	for _, col := range cols {
		if col == want {
			return true
		}
	}
	return false
}

func oneLine(text string) string {
	return compact(strings.Join(splitOutputLines(text), " "), 160)
}

func fit(text string, max int) string {
	return compact(text, max)
}

func compact(text string, max int) string {
	text = strings.Join(strings.Fields(text), " ")
	if max > 0 && len(text) > max {
		if max <= 3 {
			return text[:max]
		}
		return text[:max-3] + "..."
	}
	return text
}

func dash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

func defaultIfEmpty(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func uniqueStrings(values []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, value := range values {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func defaultKubeconfigSearchPaths() []string {
	var paths []string
	if env := os.Getenv("KUBECONFIG"); env != "" {
		paths = append(paths, filepath.SplitList(env)...)
	}
	if home, err := os.UserHomeDir(); err == nil {
		paths = append(paths, filepath.Join(home, ".kube", "config"), filepath.Join(home, ".kube", "configs.d"))
	}
	return paths
}

func discoverKubeconfigFiles(paths []string) []string {
	seen := map[string]bool{}
	var files []string
	for _, raw := range paths {
		path := expandHome(raw)
		info, err := os.Stat(path)
		if err != nil {
			continue
		}
		if !info.IsDir() {
			if !seen[path] {
				seen[path] = true
				files = append(files, path)
			}
			continue
		}
		_ = filepath.WalkDir(path, func(candidate string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return nil
			}
			name := strings.ToLower(d.Name())
			if name == "config" || strings.HasSuffix(name, ".yaml") || strings.HasSuffix(name, ".yml") || strings.HasSuffix(name, ".conf") {
				if !seen[candidate] {
					seen[candidate] = true
					files = append(files, candidate)
				}
			}
			return nil
		})
	}
	sort.Strings(files)
	return files
}

func expandHome(path string) string {
	if path == "~" {
		if home, err := os.UserHomeDir(); err == nil {
			return home
		}
	}
	if strings.HasPrefix(path, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, path[2:])
		}
	}
	return path
}

const bashCompletion = `
_kx_complete() {
  local cur="${COMP_WORDS[COMP_CWORD]}"
  local words="ctx context contexts history hist doctor why explain matrix diff drift logs log events event can access completion shell-init prompt get describe apply delete patch scale rollout"
  COMPREPLY=( $(compgen -W "$words" -- "$cur") )
}
complete -F _kx_complete kx
`

const zshCompletion = `
#compdef kx
_kx() {
  local -a commands
  commands=(
    'ctx:manage context tags, scans, and metadata'
    'why:explain why a resource is unhealthy'
    'matrix:print a fleet resource matrix'
    'diff:show sanitized resource drift'
    'drift:group contexts by sanitized resource hash'
    'logs:tail logs with context prefixes'
    'events:show recent events'
    'can:fan out kubectl auth can-i'
    'access:RBAC matrix'
    'completion:print shell completion'
    'shell-init:print completion and prompt glue'
    'prompt:print compact context prompt'
  )
  _describe 'kx command' commands
}
compdef _kx kx
`

const fishCompletion = `
complete -c kx -f -a "ctx why explain matrix diff drift logs events can access completion shell-init prompt"
`

const bashPrompt = `
_kx_prompt_segment() {
  local p
  p="$(kx prompt 2>/dev/null)" || return
  [ -n "$p" ] && printf '[%s] ' "$p"
}
PS1='$(_kx_prompt_segment)'"$PS1"
`

const zshPrompt = `
_kx_prompt_segment() {
  local p
  p="$(kx prompt 2>/dev/null)" || return
  [ -n "$p" ] && printf '[%s] ' "$p"
}
setopt PROMPT_SUBST
PROMPT='$(_kx_prompt_segment)'"$PROMPT"
`

const fishPrompt = `
function kx_prompt_segment
  set -l p (kx prompt 2>/dev/null)
  test -n "$p"; and printf '[%s] ' "$p"
end
`
