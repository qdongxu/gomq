// vhost_test.go tests VHost and VHostManager.
package server

import (
	"testing"
)

func TestNewVHostManager(t *testing.T) {
	m := NewVHostManager()
	if m.Count() != 1 {
		t.Fatalf("count = %d, want 1", m.Count())
	}
	vh, ok := m.Get("/")
	if !ok || vh.Name != "/" {
		t.Fatal("expected default VHost /")
	}
}

func TestVHostManagerCreate(t *testing.T) {
	m := NewVHostManager()
	vh, ok := m.Create("dev", "development host")
	if !ok {
		t.Fatal("expected create to succeed")
	}
	if vh.Name != "dev" {
		t.Fatalf("name = %q, want dev", vh.Name)
	}
	if vh.Description != "development host" {
		t.Fatalf("desc = %q", vh.Description)
	}
	if m.Count() != 2 {
		t.Fatalf("count = %d, want 2", m.Count())
	}

	// Duplicate creation fails.
	if _, ok := m.Create("dev", "duplicate"); ok {
		t.Fatal("expected duplicate create to fail")
	}

	// Empty name fails.
	if _, ok := m.Create("", "empty"); ok {
		t.Fatal("expected empty name create to fail")
	}

	// Default VHost "/" is protected.
	if _, ok := m.Create("/", "default"); ok {
		t.Fatal("expected default VHost create to fail")
	}
}

func TestVHostManagerDelete(t *testing.T) {
	m := NewVHostManager()
	m.Create("dev", "")

	if !m.Delete("dev") {
		t.Fatal("expected delete to succeed")
	}
	if m.Count() != 1 {
		t.Fatalf("count = %d, want 1", m.Count())
	}

	// Delete non-existent fails.
	if m.Delete("dev") {
		t.Fatal("expected delete to fail")
	}

	// Delete default VHost fails.
	if m.Delete("/") {
		t.Fatal("expected default VHost delete to fail")
	}
	if m.Count() != 1 {
		t.Fatal("expected default VHost still present")
	}
}

func TestVHostManagerList(t *testing.T) {
	m := NewVHostManager()
	m.Create("dev", "dev")
	m.Create("staging", "staging")

	list := m.List()
	if len(list) != 3 {
		t.Fatalf("list len = %d, want 3", len(list))
	}

	seen := make(map[string]bool)
	for _, vh := range list {
		seen[vh.Name] = true
	}
	if !seen["/"] || !seen["dev"] || !seen["staging"] {
		t.Fatal("expected all VHosts in list")
	}
}

func TestVHostManagerGet(t *testing.T) {
	m := NewVHostManager()
	m.Create("dev", "dev host")

	vh, ok := m.Get("dev")
	if !ok || vh.Name != "dev" {
		t.Fatal("expected to find dev")
	}

	_, ok = m.Get("nonexistent")
	if ok {
		t.Fatal("expected nonexistent not found")
	}
}
