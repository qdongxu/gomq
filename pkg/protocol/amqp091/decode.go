// decode.go reads AMQP basic types in big-endian wire format.
package amqp091

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

// Decoder deserialises AMQP basic types from a reader.
type Decoder struct {
	r io.Reader
}

// NewDecoder creates a Decoder wrapping the given reader.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: r}
}

// ReadUint8 reads a single unsigned octet.
func (d *Decoder) ReadUint8() (uint8, error) {
	var b [1]byte
	_, err := io.ReadFull(d.r, b[:])
	return b[0], err
}

// ReadUint16 reads a big-endian unsigned 16-bit integer.
func (d *Decoder) ReadUint16() (uint16, error) {
	var b [2]byte
	_, err := io.ReadFull(d.r, b[:])
	return binary.BigEndian.Uint16(b[:]), err
}

// ReadUint32 reads a big-endian unsigned 32-bit integer.
func (d *Decoder) ReadUint32() (uint32, error) {
	var b [4]byte
	_, err := io.ReadFull(d.r, b[:])
	return binary.BigEndian.Uint32(b[:]), err
}

// ReadUint64 reads a big-endian unsigned 64-bit integer.
func (d *Decoder) ReadUint64() (uint64, error) {
	var b [8]byte
	_, err := io.ReadFull(d.r, b[:])
	return binary.BigEndian.Uint64(b[:]), err
}

// ReadInt8 reads a signed 8-bit integer.
func (d *Decoder) ReadInt8() (int8, error) {
	v, err := d.ReadUint8()
	return int8(v), err
}

// ReadInt16 reads a big-endian signed 16-bit integer.
func (d *Decoder) ReadInt16() (int16, error) {
	v, err := d.ReadUint16()
	return int16(v), err
}

// ReadInt32 reads a big-endian signed 32-bit integer.
func (d *Decoder) ReadInt32() (int32, error) {
	v, err := d.ReadUint32()
	return int32(v), err
}

// ReadInt64 reads a big-endian signed 64-bit integer.
func (d *Decoder) ReadInt64() (int64, error) {
	v, err := d.ReadUint64()
	return int64(v), err
}

// ReadBool reads a single boolean octet.
func (d *Decoder) ReadBool() (bool, error) {
	v, err := d.ReadUint8()
	return v != 0, err
}

// ReadString reads a length-prefixed UTF-8 string.
func (d *Decoder) ReadString() (string, error) {
	length, err := d.ReadUint32()
	if err != nil {
		return "", err
	}
	b := make([]byte, length)
	_, err = io.ReadFull(d.r, b)
	return string(b), err
}

// ReadBytes reads a length-prefixed byte slice.
func (d *Decoder) ReadBytes() ([]byte, error) {
	length, err := d.ReadUint32()
	if err != nil {
		return nil, err
	}
	b := make([]byte, length)
	_, err = io.ReadFull(d.r, b)
	return b, err
}

// ReadTimestamp reads a Unix-epoch timestamp in seconds.
func (d *Decoder) ReadTimestamp() (time.Time, error) {
	v, err := d.ReadUint64()
	return time.Unix(int64(v), 0), err
}

// ReadTable reads an AMQP table into a Go map.
func (d *Decoder) ReadTable() (map[string]interface{}, error) {
	size, err := d.ReadUint32()
	if err != nil {
		return nil, err
	}
	if size == 0 {
		return map[string]interface{}{}, nil
	}
	data := make([]byte, size)
	_, err = io.ReadFull(d.r, data)
	if err != nil {
		return nil, err
	}
	return d.readTableBody(data)
}

// readTableBody parses the raw table bytes.
func (d *Decoder) readTableBody(data []byte) (
	map[string]interface{}, error,
) {
	r := bytes.NewReader(data)
	td := NewDecoder(r)
	result := make(map[string]interface{})
	for r.Len() > 0 {
		key, err := td.ReadString()
		if err != nil {
			return nil, fmt.Errorf(
				"table key: %w", err,
			)
		}
		val, err := td.readTableValue()
		if err != nil {
			return nil, fmt.Errorf(
				"table key %q: %w", key, err,
			)
		}
		result[key] = val
	}
	return result, nil
}

// readTableValue reads a single tagged value from a table.
func (d *Decoder) readTableValue() (interface{}, error) {
	typ, err := d.ReadUint8()
	if err != nil {
		return nil, err
	}
	switch typ {
	case 't':
		return d.ReadBool()
	case 'b':
		return d.ReadInt8()
	case 'B':
		return d.ReadUint8()
	case 's':
		return d.ReadInt16()
	case 'u':
		return d.ReadUint16()
	case 'I':
		return d.ReadInt32()
	case 'i':
		return d.ReadUint32()
	case 'l':
		return d.ReadInt64()
	case 'L':
		return d.ReadUint64()
	case 'f':
		return d.readFloat32()
	case 'd':
		return d.readFloat64()
	case 'S':
		return d.ReadString()
	case 'x':
		return d.ReadBytes()
	case 'T':
		return d.ReadTimestamp()
	case 'F':
		return d.ReadTable()
	default:
		return nil, fmt.Errorf("unknown table type %c", typ)
	}
}

// readFloat32 reads an IEEE-754 float.
func (d *Decoder) readFloat32() (float32, error) {
	v, err := d.ReadUint32()
	return float32(v), err
}

// readFloat64 reads an IEEE-754 double.
func (d *Decoder) readFloat64() (float64, error) {
	v, err := d.ReadUint64()
	return float64(v), err
}

// ReadFrame reads a complete AMQP frame from the decoder.
// It validates the frame-end octet and returns an error for
// malformed frames.
func (d *Decoder) ReadFrame(maxSize int) (*Frame, error) {
	ft, err := d.ReadUint8()
	if err != nil {
		return nil, err
	}
	ch, err := d.ReadUint16()
	if err != nil {
		return nil, err
	}
	length, err := d.ReadUint32()
	if err != nil {
		return nil, err
	}
	if int(length) > maxSize {
		return nil, fmt.Errorf(
			"frame payload %d exceeds max %d",
			length, maxSize,
		)
	}

	payload := make([]byte, length)
	_, err = io.ReadFull(d.r, payload)
	if err != nil {
		return nil, err
	}

	end, err := d.ReadUint8()
	if err != nil {
		return nil, err
	}
	if end != FrameEnd {
		return nil, fmt.Errorf(
			"frame end 0x%02X, want 0x%02X",
			end, FrameEnd,
		)
	}

	return &Frame{
		Type:    FrameType(ft),
		Channel: ch,
		Payload: payload,
	}, nil
}
