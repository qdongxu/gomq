// audit_log_test.go tests the audit log buffer.
package server

import (
	"testing"
)

func TestAuditLogDisabled(t *testing.T) {
	al := NewAuditLog(false, 10)
	al.ConnectionOpened("127.0.0.1:12345", "guest", "/")
	if al.Count() != 0 {
		t.Fatalf("expected 0 events when disabled, got %d", al.Count())
	}
}

func TestAuditLogRecordAndRecent(t *testing.T) {
	al := NewAuditLog(true, 10)
	al.ConnectionOpened("127.0.0.1:1111", "u1", "/")
	al.AuthFailure("127.0.0.1:2222", "u2", "bad password")
	al.ExchangeDeclared("u3", "127.0.0.1:3333", "/", "ex1", "direct", true)

	if al.Count() != 3 {
		t.Fatalf("expected 3 events, got %d", al.Count())
	}

	recent := al.Recent(2)
	if len(recent) != 2 {
		t.Fatalf("expected 2 recent, got %d", len(recent))
	}
	if recent[0].Category != "auth" {
		t.Fatalf("expected auth event first in recent, got %s", recent[0].Category)
	}
	if recent[1].Category != "exchange" {
		t.Fatalf("expected exchange event second, got %s", recent[1].Category)
	}
}

func TestAuditLogRingBuffer(t *testing.T) {
	al := NewAuditLog(true, 3)
	al.ConnectionOpened("a", "u", "/")
	al.ConnectionOpened("b", "u", "/")
	al.ConnectionOpened("c", "u", "/")
	al.ConnectionOpened("d", "u", "/")

	if al.Count() != 3 {
		t.Fatalf("expected 3 events after overflow, got %d", al.Count())
	}

	all := al.All()
	if all[0].Remote != "b" {
		t.Fatalf("expected oldest 'b' after drop, got %s", all[0].Remote)
	}
	if all[2].Remote != "d" {
		t.Fatalf("expected newest 'd', got %s", all[2].Remote)
	}
}

func TestAuditLogSetEnabled(t *testing.T) {
	al := NewAuditLog(false, 10)
	al.SetEnabled(true)
	al.ConnectionClosed("127.0.0.1:12345", "guest", "/")
	if al.Count() != 1 {
		t.Fatalf("expected 1 after enabling, got %d", al.Count())
	}
	al.SetEnabled(false)
	al.ConnectionOpened("127.0.0.1:9999", "guest", "/")
	if al.Count() != 1 {
		t.Fatalf("expected 1 after disabling, got %d", al.Count())
	}
}

func TestAuditLogACLAndBinding(t *testing.T) {
	al := NewAuditLog(true, 100)
	al.ACLDenied("guest", "10.0.0.1:5672", "/", "ex1", "write")
	al.BindingCreated("admin", "10.0.0.1:5672", "/", "ex1", "q1", "rk")

	all := al.All()
	if len(all) != 2 {
		t.Fatalf("expected 2 events, got %d", len(all))
	}
	if all[0].Category != "acl" || all[0].Success {
		t.Fatalf("expected ACL denial, got %+v", all[0])
	}
	if all[1].Category != "binding" || !all[1].Success {
		t.Fatalf("expected binding success, got %+v", all[1])
	}
	if all[0].Timestamp.IsZero() {
		t.Fatal("expected timestamp set")
	}
}

func TestAuditLogUnlimitedSize(t *testing.T) {
	al := NewAuditLog(true, 0)
	for i := 0; i < 5; i++ {
		al.ConnectionOpened("127.0.0.1", "guest", "/")
	}
	if al.Count() != 5 {
		t.Fatalf("expected 5 with unlimited buffer, got %d", al.Count())
	}
}
