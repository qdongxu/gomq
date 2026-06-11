// handlers_vhost_test.go tests the VHost web handlers.
package web

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

type mockVHostBroker struct {
	vhosts  []VHostListInfo
	create  func(string, string) bool
	deleteF func(string) bool
}

func (m *mockVHostBroker) VHostList() []VHostListInfo { return m.vhosts }
func (m *mockVHostBroker) CreateVHost(n, d string) bool {
	if m.create != nil {
		return m.create(n, d)
	}
	return true
}
func (m *mockVHostBroker) DeleteVHost(n string) bool {
	if m.deleteF != nil {
		return m.deleteF(n)
	}
	return true
}

func TestHandleVHostList(t *testing.T) {
	SetVHostBroker(&mockVHostBroker{
		vhosts: []VHostListInfo{
			{Name: "/", Connections: 1, Queues: 2, Exchanges: 3},
			{Name: "dev", Connections: 0, Queues: 1, Exchanges: 1},
		},
	})
	defer SetVHostBroker(nil)

	s := NewServer(AuthConfig{})
	r := httptest.NewRequest(http.MethodGet, "/api/vhosts", nil)
	w := httptest.NewRecorder()
	s.handleVHosts(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var out []VHostListInfo
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 {
		t.Fatalf("len = %d, want 2", len(out))
	}
}

func TestHandleVHostCreate(t *testing.T) {
	created := false
	SetVHostBroker(&mockVHostBroker{
		create: func(n, d string) bool {
			created = true
			return n != "existing"
		},
	})
	defer SetVHostBroker(nil)

	s := NewServer(AuthConfig{})
	body := `{"name":"dev","description":"development"}`
	r := httptest.NewRequest(http.MethodPost, "/api/vhosts", bytes.NewReader([]byte(body)))
	w := httptest.NewRecorder()
	s.handleVHosts(w, r)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", w.Code)
	}
	if !created {
		t.Fatal("expected create to be called")
	}
}

func TestHandleVHostCreateDuplicate(t *testing.T) {
	SetVHostBroker(&mockVHostBroker{
		create: func(string, string) bool { return false },
	})
	defer SetVHostBroker(nil)

	s := NewServer(AuthConfig{})
	body := `{"name":"existing","description":"dup"}`
	r := httptest.NewRequest(http.MethodPost, "/api/vhosts", bytes.NewReader([]byte(body)))
	w := httptest.NewRecorder()
	s.handleVHosts(w, r)

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", w.Code)
	}
}

func TestHandleVHostDelete(t *testing.T) {
	deleted := false
	SetVHostBroker(&mockVHostBroker{
		deleteF: func(n string) bool {
			deleted = true
			return n == "dev"
		},
	})
	defer SetVHostBroker(nil)

	s := NewServer(AuthConfig{})
	r := httptest.NewRequest(http.MethodDelete, "/api/vhosts?name=dev", nil)
	w := httptest.NewRecorder()
	s.handleVHosts(w, r)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
	if !deleted {
		t.Fatal("expected delete to be called")
	}
}

func TestHandleVHostDeleteDefault(t *testing.T) {
	SetVHostBroker(&mockVHostBroker{
		deleteF: func(string) bool { return false },
	})
	defer SetVHostBroker(nil)

	s := NewServer(AuthConfig{})
	r := httptest.NewRequest(http.MethodDelete, "/api/vhosts?name=/", nil)
	w := httptest.NewRecorder()
	s.handleVHosts(w, r)

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", w.Code)
	}
}

func TestHandleVHostDeleteMissingName(t *testing.T) {
	SetVHostBroker(&mockVHostBroker{})
	defer SetVHostBroker(nil)

	s := NewServer(AuthConfig{})
	r := httptest.NewRequest(http.MethodDelete, "/api/vhosts", nil)
	w := httptest.NewRecorder()
	s.handleVHosts(w, r)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}
