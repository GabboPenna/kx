package kube

import "testing"

func TestMatchSelectorByTag(t *testing.T) {
	state := State{
		CurrentContext: "dev-eu",
		Contexts: []Context{
			{Name: "prod-eu", Cluster: "aks-prod-weu", Namespace: "payments"},
			{Name: "dev-eu", Cluster: "kind-dev", Namespace: "default"},
		},
	}
	tags := map[string]map[string]string{
		"prod-eu": {"env": "prod", "team": "payments"},
		"dev-eu":  {"env": "dev", "team": "payments"},
	}

	matches, err := MatchSelector("@env=prod", state, tags)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 || matches[0].Name != "prod-eu" {
		t.Fatalf("expected prod-eu, got %#v", matches)
	}
}

func TestMatchSelectorByFacets(t *testing.T) {
	state := State{
		Contexts: []Context{
			{Name: "prod-eu", Cluster: "aks-prod-weu", Namespace: "payments"},
			{Name: "prod-us", Cluster: "aks-prod-use", Namespace: "payments"},
			{Name: "dev-eu", Cluster: "kind-dev", Namespace: "default"},
		},
	}
	tags := map[string]map[string]string{
		"prod-eu": {"region": "eu"},
		"prod-us": {"region": "us"},
		"dev-eu":  {"region": "eu"},
	}

	matches, err := MatchSelector("@prod.eu", state, tags)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 || matches[0].Name != "prod-eu" {
		t.Fatalf("expected prod-eu, got %#v", matches)
	}
}

func TestMatchSelectorUnionPreservesOrder(t *testing.T) {
	state := State{
		Contexts: []Context{
			{Name: "z-prod"},
			{Name: "a-dev"},
			{Name: "m-staging"},
		},
	}

	matches, err := MatchSelector("@staging,@prod", state, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 2 || matches[0].Name != "m-staging" || matches[1].Name != "z-prod" {
		t.Fatalf("unexpected order: %#v", matches)
	}
}
