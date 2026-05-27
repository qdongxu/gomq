// reloadable_test.go tests the IsReloadable helper.
package config

import (
	"testing"
)

func TestIsReloadableNoChange(t *testing.T) {
	cfg := Default()
	ok, ignored := IsReloadable(cfg, cfg)
	if !ok {
		t.Fatalf("expected reloadable when no change")
	}
	if len(ignored) != 0 {
		t.Fatalf("expected no ignored keys, got %v", ignored)
	}
}

func TestIsReloadableLogLevel(t *testing.T) {
	old := Default()
	new := Default()
	new.Log.Level = "debug"

	ok, ignored := IsReloadable(old, new)
	if !ok {
		t.Fatalf("expected reloadable for log level change")
	}
	if len(ignored) != 0 {
		t.Fatalf("expected no ignored keys, got %v", ignored)
	}
}

func TestIsReloadableTLSPaths(t *testing.T) {
	old := Default()
	new := Default()
	new.TLS.CertFile = "/new/cert.pem"

	ok, ignored := IsReloadable(old, new)
	if !ok {
		t.Fatalf("expected reloadable for tls cert_file change")
	}
	if len(ignored) != 0 {
		t.Fatalf("expected no ignored keys, got %v", ignored)
	}
}

func TestIsReloadableNetworkListeners(t *testing.T) {
	old := Default()
	new := Default()
	new.Network.Listeners = []string{"0.0.0.0:5673"}

	ok, ignored := IsReloadable(old, new)
	if ok {
		t.Fatal("expected non-reloadable for network.listeners change")
	}
	if len(ignored) == 0 {
		t.Fatal("expected ignored keys")
	}
	found := false
	for _, k := range ignored {
		if k == "network.listeners" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected network.listeners in ignored, got %v", ignored)
	}
}

func TestIsReloadableEtcdEndpoints(t *testing.T) {
	old := Default()
	new := Default()
	new.Cluster.EtcdEndpoints = []string{"http://etcd:2379"}

	ok, ignored := IsReloadable(old, new)
	if ok {
		t.Fatal("expected non-reloadable for etcd endpoints change")
	}
	found := false
	for _, k := range ignored {
		if k == "cluster.etcd_endpoints" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected cluster.etcd_endpoints in ignored, got %v", ignored)
	}
}

func TestIsReloadableMultipleChanges(t *testing.T) {
	old := Default()
	new := Default()
	new.Log.Level = "debug"
	new.Network.Listeners = []string{"0.0.0.0:5673"}
	new.Cluster.NodeID = "node-2"

	ok, ignored := IsReloadable(old, new)
	if ok {
		t.Fatal("expected non-reloadable when mixed changes")
	}
	if len(ignored) != 2 {
		t.Fatalf("expected 2 ignored keys, got %d", len(ignored))
	}
}

func TestSlicesEqual(t *testing.T) {
	if !slicesEqual([]string{"a", "b"}, []string{"a", "b"}) {
		t.Fatal("expected equal slices")
	}
	if slicesEqual([]string{"a"}, []string{"a", "b"}) {
		t.Fatal("expected different length to be unequal")
	}
	if slicesEqual([]string{"a", "b"}, []string{"b", "a"}) {
		t.Fatal("expected different order to be unequal")
	}
}
