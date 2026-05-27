// hot_reload_test.go tests the configuration reloader.
package config

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewReloader(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.toml")
	_ = os.WriteFile(path, []byte(""), 0644)

	loader := func(p string) (*Config, error) {
		return Default(), nil
	}
	applier := func(cfg *Config) error { return nil }

	r, err := NewReloader(path, loader, applier)
	if err != nil {
		t.Fatalf("new reloader: %v", err)
	}
	defer r.Stop()

	// Immediate reload should work.
	if err := r.Reload(); err != nil {
		t.Fatalf("reload: %v", err)
	}
}

func TestReloaderReloadOnce(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.toml")
	_ = os.WriteFile(path, []byte(""), 0644)

	callCount := 0
	loader := func(p string) (*Config, error) {
		callCount++
		return Default(), nil
	}
	applier := func(cfg *Config) error { return nil }

	r, _ := NewReloader(path, loader, applier)
	defer r.Stop()

	// First reload loads and applies.
	if err := r.Reload(); err != nil {
		t.Fatalf("first reload: %v", err)
	}
	if callCount != 1 {
		t.Fatalf("expected 1 load call, got %d", callCount)
	}

	// Second reload without file change should be a no-op (mtime check).
	if err := r.Reload(); err != nil {
		t.Fatalf("second reload: %v", err)
	}
	if callCount != 1 {
		t.Fatalf("expected 1 load call after no-op reload, got %d", callCount)
	}

	// Touch the file to change mtime.
	_ = os.WriteFile(path, []byte(""), 0644)
	if err := r.Reload(); err != nil {
		t.Fatalf("third reload after touch: %v", err)
	}
	if callCount != 2 {
		t.Fatalf("expected 2 load calls after touch, got %d", callCount)
	}
}

func TestReloaderLoaderError(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.toml")
	_ = os.WriteFile(path, []byte(""), 0644)

	loader := func(p string) (*Config, error) {
		return nil, errors.New("load error")
	}
	applier := func(cfg *Config) error { return nil }

	r, _ := NewReloader(path, loader, applier)
	defer r.Stop()

	if err := r.Reload(); err == nil {
		t.Fatal("expected error from loader")
	}
}

func TestReloaderApplierError(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.toml")
	_ = os.WriteFile(path, []byte(""), 0644)

	loader := func(p string) (*Config, error) {
		return Default(), nil
	}
	applier := func(cfg *Config) error {
		return errors.New("apply error")
	}

	r, _ := NewReloader(path, loader, applier)
	defer r.Stop()

	if err := r.Reload(); err == nil {
		t.Fatal("expected error from applier")
	}
}

func TestReloaderStartStop(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.toml")
	_ = os.WriteFile(path, []byte(""), 0644)

	loader := func(p string) (*Config, error) { return Default(), nil }
	applier := func(cfg *Config) error { return nil }

	r, err := NewReloader(path, loader, applier)
	if err != nil {
		t.Fatalf("new reloader: %v", err)
	}

	r.Start()
	time.Sleep(50 * time.Millisecond)
	r.Stop()

	// Stop should be idempotent.
	r.Stop()
}
