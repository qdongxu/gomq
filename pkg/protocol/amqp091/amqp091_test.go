package amqp091

import (
	"bufio"
	"bytes"
	"testing"
	"time"
)

// TestFrameEncodeDecode verifies that a frame survives encode/decode.
func TestFrameEncodeDecode(t *testing.T) {
	f := &Frame{
		Type:    FrameMethod,
		Channel: 1,
		Payload: []byte("hello"),
	}

	enc := NewEncoder()
	if err := enc.EncodeFrame(f); err != nil {
		t.Fatalf("encode: %v", err)
	}

	dec := NewDecoder(bytes.NewReader(enc.Bytes()))
	got, err := dec.ReadFrame(FrameMaxDefault)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.Type != f.Type {
		t.Errorf("type = %d, want %d", got.Type, f.Type)
	}
	if got.Channel != f.Channel {
		t.Errorf("channel = %d, want %d", got.Channel, f.Channel)
	}
	if !bytes.Equal(got.Payload, f.Payload) {
		t.Errorf("payload = %q, want %q", got.Payload, f.Payload)
	}
}

// TestBasicTypesRoundTrip exercises every encoder/decoder pair.
func TestBasicTypesRoundTrip(t *testing.T) {
	enc := NewEncoder()

	enc.WriteBool(true)
	enc.WriteUint8(42)
	enc.WriteUint16(1000)
	enc.WriteUint32(50000)
	enc.WriteUint64(999999)
	enc.WriteInt8(-5)
	enc.WriteInt16(-100)
	enc.WriteInt32(-5000)
	enc.WriteInt64(-99999)
	enc.WriteString("hello")
	enc.WriteBytes([]byte{1, 2, 3})
	enc.WriteTimestamp(time.Unix(1234567890, 0))
	enc.WriteTable(map[string]interface{}{
		"a": true,
		"b": int32(7),
		"c": "nested",
		"d": map[string]interface{}{
			"e": int64(99),
		},
	})

	dec := NewDecoder(bytes.NewReader(enc.Bytes()))

	cases := []struct {
		name string
		got  interface{}
		want interface{}
	}{
		{"bool", must(dec.ReadBool()), true},
		{"uint8", must(dec.ReadUint8()), uint8(42)},
		{"uint16", must(dec.ReadUint16()), uint16(1000)},
		{"uint32", must(dec.ReadUint32()), uint32(50000)},
		{"uint64", must(dec.ReadUint64()), uint64(999999)},
		{"int8", must(dec.ReadInt8()), int8(-5)},
		{"int16", must(dec.ReadInt16()), int16(-100)},
		{"int32", must(dec.ReadInt32()), int32(-5000)},
		{"int64", must(dec.ReadInt64()), int64(-99999)},
		{"string", must(dec.ReadString()), "hello"},
		{"bytes", must(dec.ReadBytes()), []byte{1, 2, 3}},
		{"timestamp", must(dec.ReadTimestamp()), time.Unix(1234567890, 0)},
	}

	for _, c := range cases {
		if !equal(c.got, c.want) {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}

	tab, err := dec.ReadTable()
	if err != nil {
		t.Fatalf("read table: %v", err)
	}
	if tab["a"] != true {
		t.Errorf("table a = %v", tab["a"])
	}
	nested, ok := tab["d"].(map[string]interface{})
	if !ok {
		t.Fatal("nested table missing")
	}
	if nested["e"] != int64(99) {
		t.Errorf("nested e = %v", nested["e"])
	}
}

// TestFramerReadMultiple checks that Framer parses back-to-back frames.
func TestFramerReadMultiple(t *testing.T) {
	var buf bytes.Buffer
	for i := 0; i < 3; i++ {
		enc := NewEncoder()
		enc.EncodeFrame(&Frame{
			Type:    FrameBody,
			Channel: uint16(i),
			Payload: []byte{byte(i)},
		})
		buf.Write(enc.Bytes())
	}

	fr := NewFramer(bufio.NewReader(&buf), FrameMaxDefault)
	for i := 0; i < 3; i++ {
		f, err := fr.NextFrame()
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if f.Channel != uint16(i) {
			t.Errorf("frame %d channel = %d", i, f.Channel)
		}
	}
}

// TestInvalidFrameEnd rejects a frame with a wrong trailer octet.
func TestInvalidFrameEnd(t *testing.T) {
	data := []byte{1, 0, 0, 0, 0, 0, 1, 'x', 0xFF}
	dec := NewDecoder(bytes.NewReader(data))
	_, err := dec.ReadFrame(FrameMaxDefault)
	if err == nil {
		t.Fatal("expected error for bad frame end")
	}
}

// TestFrameTooLarge rejects a frame whose payload exceeds the limit.
func TestFrameTooLarge(t *testing.T) {
	data := []byte{1, 0, 0, 0, 0, 0, 10, 1, 2, 3, 0xCE}
	dec := NewDecoder(bytes.NewReader(data))
	_, err := dec.ReadFrame(5)
	if err == nil {
		t.Fatal("expected error for oversized frame")
	}
}

// TestEmptyPayload accepts a frame with zero-length payload.
func TestEmptyPayload(t *testing.T) {
	f := &Frame{
		Type:    FrameHeartbeat,
		Channel: 0,
		Payload: []byte{},
	}
	enc := NewEncoder()
	enc.EncodeFrame(f)

	dec := NewDecoder(bytes.NewReader(enc.Bytes()))
	got, err := dec.ReadFrame(FrameMaxDefault)
	if err != nil {
		t.Fatalf("decode empty payload: %v", err)
	}
	if len(got.Payload) != 0 {
		t.Errorf("payload len = %d, want 0", len(got.Payload))
	}
}

// TestFramerInvalidType rejects an unknown frame type.
func TestFramerInvalidType(t *testing.T) {
	data := []byte{99, 0, 0, 0, 0, 0, 0, 0xCE}
	fr := NewFramer(bufio.NewReader(bytes.NewReader(data)), FrameMaxDefault)
	_, err := fr.NextFrame()
	if err == nil {
		t.Fatal("expected error for invalid frame type")
	}
}

// must is a test helper that panics on error.
func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// equal compares two values for equality.
func equal(a, b interface{}) bool {
	switch av := a.(type) {
	case []byte:
		bv, ok := b.([]byte)
		return ok && bytes.Equal(av, bv)
	case time.Time:
		bv, ok := b.(time.Time)
		return ok && av.Equal(bv)
	default:
		return a == b
	}
}
