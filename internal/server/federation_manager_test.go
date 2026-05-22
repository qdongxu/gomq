// federation_manager_test.go tests the FederationManager.
package server

import "testing"

func TestFederationManager_Add(t *testing.T) {
	fm := NewFederationManager()
	cfg := NewFederationConfig("f1")
	fm.Add(cfg)

	if fm.Count() != 1 {
		t.Fatalf("expected count 1, got %d", fm.Count())
	}
}

func TestFederationManager_AddNil(t *testing.T) {
	fm := NewFederationManager()
	fm.Add(nil)
	if fm.Count() != 0 {
		t.Fatalf("expected count 0 after nil, got %d", fm.Count())
	}
}

func TestFederationManager_Remove(t *testing.T) {
	fm := NewFederationManager()
	fm.Add(NewFederationConfig("f1"))
	if !fm.Remove("f1") {
		t.Fatal("expected Remove to return true")
	}
	if fm.Count() != 0 {
		t.Fatalf("expected count 0, got %d", fm.Count())
	}
}

func TestFederationManager_List(t *testing.T) {
	fm := NewFederationManager()
	fm.Add(NewFederationConfig("a"))
	fm.Add(NewFederationConfig("b"))

	list := fm.List()
	if len(list) != 2 {
		t.Fatalf("expected 2 configs, got %d", len(list))
	}
}

func TestFederationManager_Get(t *testing.T) {
	fm := NewFederationManager()
	fm.Add(NewFederationConfig("found"))

	cfg, ok := fm.Get("found")
	if !ok || cfg.Name != "found" {
		t.Fatal("expected to find config")
	}

	_, ok = fm.Get("missing")
	if ok {
		t.Fatal("expected missing not found")
	}
}

func TestFederationManager_StartAll(t *testing.T) {
	fm := NewFederationManager()
	fm.Add(NewFederationConfig("fed-1"))
	fm.StartAll() // should not panic
}
