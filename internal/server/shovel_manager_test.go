// shovel_manager_test.go tests the ShovelManager.
package server

import "testing"

func TestShovelManager_Add(t *testing.T) {
	sm := NewShovelManager()
	sm.Add(NewShovel("s1", "src", "dst"))

	if sm.Count() != 1 {
		t.Fatalf("expected count 1, got %d", sm.Count())
	}
}

func TestShovelManager_AddNil(t *testing.T) {
	sm := NewShovelManager()
	sm.Add(nil)
	if sm.Count() != 0 {
		t.Fatalf("expected count 0 after nil, got %d", sm.Count())
	}
}

func TestShovelManager_Remove(t *testing.T) {
	sm := NewShovelManager()
	sm.Add(NewShovel("s1", "src", "dst"))
	if !sm.Remove("s1") {
		t.Fatal("expected Remove to return true")
	}
	if sm.Count() != 0 {
		t.Fatalf("expected count 0, got %d", sm.Count())
	}
}

func TestShovelManager_List(t *testing.T) {
	sm := NewShovelManager()
	sm.Add(NewShovel("a", "s1", "d1"))
	sm.Add(NewShovel("b", "s2", "d2"))

	list := sm.List()
	if len(list) != 2 {
		t.Fatalf("expected 2 shovels, got %d", len(list))
	}
}

func TestShovelManager_Get(t *testing.T) {
	sm := NewShovelManager()
	sm.Add(NewShovel("found", "src", "dst"))

	s, ok := sm.Get("found")
	if !ok || s.Name != "found" {
		t.Fatal("expected to find shovel")
	}

	_, ok = sm.Get("missing")
	if ok {
		t.Fatal("expected missing not found")
	}
}

func TestShovelManager_StartAll(t *testing.T) {
	sm := NewShovelManager()
	sm.Add(NewShovel("sh-1", "src", "dst"))
	sm.StartAll() // should not panic

	s, _ := sm.Get("sh-1")
	if s.Status() != ShovelRunning {
		t.Fatalf("expected running, got %s", s.Status().String())
	}
}
