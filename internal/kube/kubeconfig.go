package kube

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
)

type State struct {
	CurrentContext string
	Contexts       []Context
}

type Context struct {
	Name      string
	Cluster   string
	User      string
	Namespace string
}

type rawKubeConfig struct {
	CurrentContext string `json:"current-context"`
	Contexts       []struct {
		Name    string `json:"name"`
		Context struct {
			Cluster   string `json:"cluster"`
			User      string `json:"user"`
			Namespace string `json:"namespace"`
		} `json:"context"`
	} `json:"contexts"`
}

// Load asks kubectl to parse kubeconfig. I would rather trust kubectl here than guess every kubeconfig edge case.
func Load(kubectlPath string) (State, error) {
	return LoadWithPrefix(kubectlPath, nil)
}

func LoadWithPrefix(kubectlPath string, prefix []string) (State, error) {
	args := append([]string{}, prefix...)
	args = append(args, "config", "view", "-o", "json")
	cmd := exec.Command(kubectlPath, args...)
	var out bytes.Buffer
	var errOut bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	if err := cmd.Run(); err != nil {
		return State{}, fmt.Errorf("kubectl config view failed: %s", strings.TrimSpace(errOut.String()))
	}

	var raw rawKubeConfig
	if err := json.Unmarshal(out.Bytes(), &raw); err != nil {
		return State{}, fmt.Errorf("cannot decode kubeconfig json: %w", err)
	}

	state := State{CurrentContext: raw.CurrentContext}
	for _, item := range raw.Contexts {
		state.Contexts = append(state.Contexts, Context{
			Name:      item.Name,
			Cluster:   item.Context.Cluster,
			User:      item.Context.User,
			Namespace: item.Context.Namespace,
		})
	}
	return state, nil
}

func (s State) HasContext(name string) bool {
	_, ok := s.ContextByName(name)
	return ok
}

func (s State) ContextByName(name string) (Context, bool) {
	for _, ctx := range s.Contexts {
		if ctx.Name == name {
			return ctx, true
		}
	}
	return Context{}, false
}

func MatchSelector(selector string, state State, tags map[string]map[string]string) ([]Context, error) {
	if !strings.HasPrefix(selector, "@") {
		selector = "@" + selector
	}

	seen := map[string]bool{}
	var matches []Context
	for _, part := range strings.Split(strings.TrimPrefix(selector, "@"), ",") {
		part = strings.TrimPrefix(strings.TrimSpace(part), "@")
		if part == "" {
			continue
		}
		partMatches, err := matchOne(part, state, tags)
		if err != nil {
			return nil, err
		}
		for _, ctx := range partMatches {
			if !seen[ctx.Name] {
				seen[ctx.Name] = true
				matches = append(matches, ctx)
			}
		}
	}
	return matches, nil
}

func matchOne(selector string, state State, tags map[string]map[string]string) ([]Context, error) {
	switch selector {
	case "all", "*":
		return state.Contexts, nil
	case "current", ".":
		if state.CurrentContext == "" {
			return nil, nil
		}
		ctx, ok := state.ContextByName(state.CurrentContext)
		if !ok {
			return nil, nil
		}
		return []Context{ctx}, nil
	}

	if strings.HasPrefix(selector, "/") && strings.HasSuffix(selector, "/") && len(selector) > 2 {
		re, err := regexp.Compile(strings.Trim(selector, "/"))
		if err != nil {
			return nil, fmt.Errorf("invalid selector regex: %w", err)
		}
		var matches []Context
		for _, ctx := range state.Contexts {
			if re.MatchString(ctx.Name) {
				matches = append(matches, ctx)
			}
		}
		return matches, nil
	}

	if key, value, ok := splitTagSelector(selector); ok {
		var matches []Context
		for _, ctx := range state.Contexts {
			if tagValue(tags, ctx.Name, key) == value {
				matches = append(matches, ctx)
			}
		}
		return matches, nil
	}

	facets := strings.Split(selector, ".")
	var matches []Context
	for _, ctx := range state.Contexts {
		if contextMatchesAllFacets(ctx, tags[ctx.Name], facets) {
			matches = append(matches, ctx)
		}
	}
	return matches, nil
}

func splitTagSelector(selector string) (string, string, bool) {
	if key, value, ok := strings.Cut(selector, "="); ok && key != "" {
		return key, value, true
	}
	if key, value, ok := strings.Cut(selector, ":"); ok && key != "" {
		return key, value, true
	}
	return "", "", false
}

func contextMatchesAllFacets(ctx Context, tags map[string]string, facets []string) bool {
	for _, facet := range facets {
		if facet == "" {
			continue
		}
		if !contextMatchesFacet(ctx, tags, strings.ToLower(facet)) {
			return false
		}
	}
	return true
}

func contextMatchesFacet(ctx Context, tags map[string]string, facet string) bool {
	fields := []string{ctx.Name, ctx.Cluster, ctx.User, ctx.Namespace}
	for _, field := range fields {
		if strings.Contains(strings.ToLower(field), facet) {
			return true
		}
	}
	for key, value := range tags {
		if strings.Contains(strings.ToLower(key), facet) || strings.Contains(strings.ToLower(value), facet) {
			return true
		}
	}
	return false
}

func tagValue(tags map[string]map[string]string, contextName string, key string) string {
	if tags[contextName] == nil {
		return ""
	}
	return tags[contextName][key]
}
