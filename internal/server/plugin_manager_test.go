// plugin_manager_test.go tests the PluginManager.
package server

import (
	"errors"
	"testing"

	"github.com/qdongxu/gomq/pkg/plugin"
)

// mockPlugin is a test implementation of plugin.Plugin.
type mockPlugin struct {
	name       string
	initCalled bool
	initErr    error
}

func (m *mockPlugin) Name() string { return m.name }
func (m *mockPlugin) Init(_ plugin.ServerLike) error {
	m.initCalled = true
	return m.initErr
}

func TestPluginManager_Register(t *testing.T) {
	pm := NewPluginManager()
	p := &mockPlugin{name: "test-a"}
	pm.Register(p)

	if pm.Count() != 1 {
		t.Fatalf("expected count 1, got %d", pm.Count())
	}
}

func TestPluginManager_RegisterNil(t *testing.T) {
	pm := NewPluginManager()
	pm.Register(nil)

	if pm.Count() != 0 {
		t.Fatalf("expected count 0 after nil, got %d", pm.Count())
	}
}

func TestPluginManager_LoadAll(t *testing.T) {
	pm := NewPluginManager()
	pm.Register(&mockPlugin{name: "p1"})
	pm.Register(&mockPlugin{name: "p2"})

	names := pm.LoadAll()
	if len(names) != 2 {
		t.Fatalf("expected 2 names, got %d", len(names))
	}
}

func TestPluginManager_InitAll(t *testing.T) {
	pm := NewPluginManager()
	p1 := &mockPlugin{name: "p1"}
	p2 := &mockPlugin{name: "p2", initErr: errors.New("boom")}
	pm.Register(p1)
	pm.Register(p2)

	pm.InitAll(nil) // srv not used by mock

	if !p1.initCalled {
		t.Fatal("expected p1.Init to be called")
	}
	if !p2.initCalled {
		t.Fatal("expected p2.Init to be called")
	}
}

func TestPluginManager_Names(t *testing.T) {
	pm := NewPluginManager()
	pm.Register(&mockPlugin{name: "alpha"})

	names := pm.Names()
	if len(names) != 1 || names[0] != "alpha" {
		t.Fatalf("unexpected names: %v", names)
	}
}

func TestPluginManager_Get(t *testing.T) {
	pm := NewPluginManager()
	p := &mockPlugin{name: "found"}
	pm.Register(p)

	got, ok := pm.Get("found")
	if !ok {
		t.Fatal("expected to find plugin")
	}
	if got.Name() != "found" {
		t.Fatalf("unexpected name: %s", got.Name())
	}

	_, ok = pm.Get("missing")
	if ok {
		t.Fatal("expected missing plugin not found")
	}
}

func TestPluginManager_Unregister(t *testing.T) {
	pm := NewPluginManager()
	pm.Register(&mockPlugin{name: "remove-me"})

	if !pm.Unregister("remove-me") {
		t.Fatal("expected unregister to return true")
	}
	if pm.Count() != 0 {
		t.Fatalf("expected count 0, got %d", pm.Count())
	}

	if pm.Unregister("not-there") {
		t.Fatal("expected unregister to return false")
	}
}
