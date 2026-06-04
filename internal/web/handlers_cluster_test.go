// handlers_cluster_test.go tests the cluster web UI handler.
package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

type mockClusterBroker struct {
	nodes []ClusterNodeInfo
}

func (m *mockClusterBroker) ClusterList() []ClusterNodeInfo {
	return m.nodes
}

func TestHandleCluster(t *testing.T) {
	SetClusterBroker(&mockClusterBroker{
		nodes: []ClusterNodeInfo{
			{ID: "node-1", Addr: "192.168.1.1:7946",
				Status: "alive", Role: "leader",
				Uptime: "2h30m", Conns: 42,
				MsgIn: 1000, MsgOut: 950,
				LogIndex: 128, LogTerm: 3,
				Heartbeat: "0ms"},
			{ID: "node-2", Addr: "192.168.1.2:7946",
				Status: "alive", Role: "follower",
				Uptime: "2h15m", Conns: 35,
				MsgIn: 800, MsgOut: 820,
				LogIndex: 128, LogTerm: 3,
				Heartbeat: "2ms"},
		},
	})
	defer SetClusterBroker(nil)

	srv := NewServer(AuthConfig{Username: "admin", Password: "admin"})
	req := httptest.NewRequest("GET", "/api/cluster", nil)
	w := httptest.NewRecorder()
	req.AddCookie(loginCookie(srv))
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var out []ClusterNodeInfo
	if err := json.NewDecoder(w.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("unexpected len: %d", len(out))
	}
	if out[0].ID != "node-1" || out[0].Role != "leader" {
		t.Fatalf("unexpected response: %+v", out[0])
	}
	if out[1].ID != "node-2" || out[1].Role != "follower" {
		t.Fatalf("unexpected response: %+v", out[1])
	}
	if out[0].MsgIn != 1000 || out[1].Heartbeat != "2ms" {
		t.Fatalf("unexpected stats: %+v %+v", out[0], out[1])
	}
}

func TestHandleClusterEmpty(t *testing.T) {
	SetClusterBroker(&mockClusterBroker{nodes: []ClusterNodeInfo{}})
	defer SetClusterBroker(nil)

	srv := NewServer(AuthConfig{Username: "admin", Password: "admin"})
	req := httptest.NewRequest("GET", "/api/cluster", nil)
	w := httptest.NewRecorder()
	req.AddCookie(loginCookie(srv))
	srv.ServeHTTP(w, req)

	var out []ClusterNodeInfo
	if err := json.NewDecoder(w.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("expected empty, got %d", len(out))
	}
}

func TestHandleClusterNoBroker(t *testing.T) {
	SetClusterBroker(nil)

	srv := NewServer(AuthConfig{Username: "admin", Password: "admin"})
	req := httptest.NewRequest("GET", "/api/cluster", nil)
	w := httptest.NewRecorder()
	req.AddCookie(loginCookie(srv))
	srv.ServeHTTP(w, req)

	var out []ClusterNodeInfo
	if err := json.NewDecoder(w.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("expected empty, got %d", len(out))
	}
}
