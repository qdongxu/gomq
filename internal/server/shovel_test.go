// shovel_test.go tests the Shovel type and ShovelStatus.
package server

import "testing"

func TestNewShovel(t *testing.T) {
	s := NewShovel("sh-1", "amqp://src:5672", "amqp://dst:5672")
	if s.Name != "sh-1" {
		t.Fatalf("expected name sh-1, got %q", s.Name)
	}
	if s.Status() != ShovelStopped {
		t.Fatalf("expected status stopped, got %s", s.Status().String())
	}
}

func TestShovel_Run(t *testing.T) {
	s := NewShovel("sh-2", "src", "dst")
	if err := s.Run(); err != nil {
		t.Fatalf("run: %v", err)
	}
	if s.Status() != ShovelRunning {
		t.Fatalf("expected status running, got %s", s.Status().String())
	}
}

func TestShovel_Stop(t *testing.T) {
	s := NewShovel("sh-3", "src", "dst")
	_ = s.Run()
	s.Stop()
	if s.Status() != ShovelStopped {
		t.Fatalf("expected status stopped, got %s", s.Status().String())
	}
}

func TestShovelStatus_String(t *testing.T) {
	if ShovelRunning.String() != "running" {
		t.Fatalf("unexpected running string: %s", ShovelRunning.String())
	}
	if ShovelStopped.String() != "stopped" {
		t.Fatalf("unexpected stopped string: %s", ShovelStopped.String())
	}
	if ShovelError.String() != "error" {
		t.Fatalf("unexpected error string: %s", ShovelError.String())
	}
}
