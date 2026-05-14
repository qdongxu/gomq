// encode.go writes AMQP basic types in big-endian wire format.
package amqp091

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
)

// Encoder serialises AMQP basic types into a buffer.
type Encoder struct {
	buf bytes.Buffer
}

// NewEncoder creates an empty Encoder.
func NewEncoder() *Encoder {
	return &Encoder{}
}

// Bytes returns the accumulated encoded data.
func (e *Encoder) Bytes() []byte {
	return e.buf.Bytes()
}

// Reset discards all accumulated data.
func (e *Encoder) Reset() {
	e.buf.Reset()
}

// WriteBool encodes a single boolean octet.
func (e *Encoder) WriteBool(v bool) error {
	if v {
		return e.WriteUint8(1)
	}
	return e.WriteUint8(0)
}

// WriteUint8 encodes an unsigned 8-bit integer.
func (e *Encoder) WriteUint8(v uint8) error {
	e.buf.WriteByte(v)
	return nil
}

// WriteUint16 encodes an unsigned 16-bit integer in big-endian.
func (e *Encoder) WriteUint16(v uint16) error {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, v)
	_, err := e.buf.Write(b)
	return err
}

// WriteUint32 encodes an unsigned 32-bit integer in big-endian.
func (e *Encoder) WriteUint32(v uint32) error {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	_, err := e.buf.Write(b)
	return err
}

// WriteUint64 encodes an unsigned 64-bit integer in big-endian.
func (e *Encoder) WriteUint64(v uint64) error {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	_, err := e.buf.Write(b)
	return err
}

// WriteInt8 encodes a signed 8-bit integer.
func (e *Encoder) WriteInt8(v int8) error {
	return e.WriteUint8(uint8(v))
}

// WriteInt16 encodes a signed 16-bit integer in big-endian.
func (e *Encoder) WriteInt16(v int16) error {
	return e.WriteUint16(uint16(v))
}

// WriteInt32 encodes a signed 32-bit integer in big-endian.
func (e *Encoder) WriteInt32(v int32) error {
	return e.WriteUint32(uint32(v))
}

// WriteInt64 encodes a signed 64-bit integer in big-endian.
func (e *Encoder) WriteInt64(v int64) error {
	return e.WriteUint64(uint64(v))
}

// WriteShortString encodes a 1-byte-length-prefixed string.
func (e *Encoder) WriteShortString(v string) error {
	b := []byte(v)
	if err := e.WriteUint8(uint8(len(b))); err != nil {
		return err
	}
	_, err := e.buf.Write(b)
	return err
}

// WriteString encodes a length-prefixed UTF-8 string.
// The length prefix is a 32-bit unsigned integer.
func (e *Encoder) WriteString(v string) error {
	b := []byte(v)
	if err := e.WriteUint32(uint32(len(b))); err != nil {
		return err
	}
	_, err := e.buf.Write(b)
	return err
}

// WriteBytes encodes a length-prefixed byte slice.
func (e *Encoder) WriteBytes(v []byte) error {
	if err := e.WriteUint32(uint32(len(v))); err != nil {
		return err
	}
	_, err := e.buf.Write(v)
	return err
}

// WriteTimestamp encodes a time as Unix seconds (uint64).
func (e *Encoder) WriteTimestamp(v time.Time) error {
	return e.WriteUint64(uint64(v.Unix()))
}

// TableValue is a single value inside an AMQP table.
type TableValue struct {
	Type  byte        // AMQP type tag
	Value interface{} // Go value
}

// WriteTable encodes an AMQP table (map of string to typed values).
// Supported Go types: bool, int8, uint8, int16, uint16, int32,
// uint32, int64, uint64, float32, float64, string, []byte,
// time.Time, and map[string]interface{} (nested table).
func (e *Encoder) WriteTable(v map[string]interface{}) error {
	// Encode table into a temporary encoder so we can prefix the
	// total size.
	tmp := NewEncoder()
	for key, val := range v {
		if err := tmp.WriteString(key); err != nil {
			return err
		}
		if err := tmp.writeTableValue(val); err != nil {
			return fmt.Errorf("table key %q: %w", key, err)
		}
	}
	data := tmp.Bytes()
	if err := e.WriteUint32(uint32(len(data))); err != nil {
		return err
	}
	_, err := e.buf.Write(data)
	return err
}

// writeTableValue encodes a single table element with its type tag.
func (e *Encoder) writeTableValue(v interface{}) error {
	switch val := v.(type) {
	case bool:
		e.WriteUint8('t')
		return e.WriteBool(val)
	case int8:
		e.WriteUint8('b')
		return e.WriteInt8(val)
	case uint8:
		e.WriteUint8('B')
		return e.WriteUint8(val)
	case int16:
		e.WriteUint8('s')
		return e.WriteInt16(val)
	case uint16:
		e.WriteUint8('u')
		return e.WriteUint16(val)
	case int32:
		e.WriteUint8('I')
		return e.WriteInt32(val)
	case uint32:
		e.WriteUint8('i')
		return e.WriteUint32(val)
	case int64:
		e.WriteUint8('l')
		return e.WriteInt64(val)
	case uint64:
		e.WriteUint8('L')
		return e.WriteUint64(val)
	case float32:
		e.WriteUint8('f')
		return e.writeFloat32(val)
	case float64:
		e.WriteUint8('d')
		return e.writeFloat64(val)
	case string:
		e.WriteUint8('S')
		return e.WriteString(val)
	case []byte:
		e.WriteUint8('x')
		return e.WriteBytes(val)
	case time.Time:
		e.WriteUint8('T')
		return e.WriteTimestamp(val)
	case map[string]interface{}:
		e.WriteUint8('F')
		return e.WriteTable(val)
	default:
		return fmt.Errorf("unsupported table value type %T", v)
	}
}

// writeFloat32 encodes an IEEE-754 float in big-endian.
func (e *Encoder) writeFloat32(v float32) error {
	return e.WriteUint32(
		uint32(v),
	)
}

// writeFloat64 encodes an IEEE-754 double in big-endian.
func (e *Encoder) writeFloat64(v float64) error {
	return e.WriteUint64(
		uint64(v),
	)
}

// EncodeFrame serialises a Frame into the encoder buffer.
func (e *Encoder) EncodeFrame(f *Frame) error {
	if !f.IsValidType() {
		return fmt.Errorf("invalid frame type %d", f.Type)
	}
	if err := e.WriteUint8(uint8(f.Type)); err != nil {
		return err
	}
	if err := e.WriteUint16(f.Channel); err != nil {
		return err
	}
	if err := e.WriteUint32(uint32(len(f.Payload))); err != nil {
		return err
	}
	if _, err := e.buf.Write(f.Payload); err != nil {
		return err
	}
	return e.WriteUint8(FrameEnd)
}
