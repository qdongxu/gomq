// backpressure_test.go tests memory backpressure control.
package server

import "testing"

func TestBackPressureDisabled(t *testing.T) {
	bp := NewBackPressure(0)
	if !bp.CanAccept() {
		t.Fatalf("disabled backpressure should always allow")
	}
	if bp.Check() {
		t.Fatalf("disabled should never report pressure")
	}
}

func TestBackPressureSimulated(t *testing.T) {
	bp := NewBackPressure(80)
	bp.SetSimulatedUsage(50)
	if !bp.CanAccept() {
		t.Fatalf("expected accept below threshold")
	}
	if bp.Check() {
		t.Fatalf("expected no pressure below threshold")
	}

	bp.SetSimulatedUsage(90)
	if !bp.Check() {
		t.Fatalf("expected pressure above threshold")
	}
	if bp.CanAccept() {
		t.Fatalf("expected deny above threshold")
	}
}

func TestBackPressureSetThreshold(t *testing.T) {
	bp := NewBackPressure(80)
	bp.SetSimulatedUsage(70)
	if bp.Check() {
		t.Fatalf("expected no pressure at 70 < 80")
	}

	bp.SetThreshold(60)
	if !bp.Check() {
		t.Fatalf("expected pressure at 70 > 60")
	}
}
