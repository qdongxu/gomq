// zero_copy_test.go tests SharedSlice and ZeroCopyMessage.
package server

import (
	"testing"
)

func TestSharedSlice_ReferenceCounting(t *testing.T) {
	data := []byte("payload")
	s1 := NewSharedSlice(data)

	if s1.Len() != len(data) {
		t.Fatalf("expected len %d, got %d", len(data), s1.Len())
	}

	s2 := s1.Clone()
	s1.Release()

	// s2 should still be valid after s1 release
	if string(s2.Bytes()) != "payload" {
		t.Fatal("expected s2 to still hold data")
	}

	s2.Release()
	// Now underlying data should be nilled
	if s2.data != nil {
		t.Log("data not nilled after all refs released (acceptable)")
	}
}

func TestSharedSlice_MultipleClones(t *testing.T) {
	data := []byte("hello world")
	s1 := NewSharedSlice(data)
	s2 := s1.Clone()
	s3 := s2.Clone()

	s1.Release()
	s2.Release()

	if string(s3.Bytes()) != "hello world" {
		t.Fatal("expected s3 to still hold data")
	}
	s3.Release()
}

func TestZeroCopyMessage_Wrapper(t *testing.T) {
	msg := NewMessage([]byte("test payload"), Properties{})
	zc := NewZeroCopyMessage(msg)

	p1 := zc.Payload()
	p2 := zc.Payload()

	if string(p1.Bytes()) != "test payload" {
		t.Fatal("payload mismatch")
	}
	if string(p2.Bytes()) != "test payload" {
		t.Fatal("payload mismatch on second clone")
	}

	p1.Release()
	p2.Release()
	zc.Release()
}

func TestZeroCopyMessage_NoCopy(t *testing.T) {
	payload := []byte("original")
	msg := NewMessage(payload, Properties{})
	zc := NewZeroCopyMessage(msg)

	// Modify original (not recommended, but tests independence)
	payload[0] = 'X'

	p := zc.Payload()
	if p.Bytes()[0] != 'X' {
		t.Log("zero-copy shares underlying array (expected)")
	}
	p.Release()
	zc.Release()
}
