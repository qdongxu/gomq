package amqp091

import (
	"bufio"
	"fmt"
)

// Framer reads AMQP frames incrementally from a buffered reader.
// It is used by the connection layer to avoid loading the entire
// TCP stream into memory.
type Framer struct {
	reader  *bufio.Reader
	maxSize int
}

// NewFramer creates a Framer wrapping the given reader.
// maxSize is the maximum allowed payload size in bytes.
func NewFramer(r *bufio.Reader, maxSize int) *Framer {
	if maxSize < FrameMinSize {
		maxSize = FrameMaxDefault
	}
	return &Framer{
		reader:  r,
		maxSize: maxSize,
	}
}

// NextFrame reads the next complete frame from the wire.
// It blocks until at least one full frame is available.
// Returns io.EOF only when the underlying reader closes.
func (f *Framer) NextFrame() (*Frame, error) {
	// We need at least 7 bytes: type(1) + channel(2) + length(4).
	// Using Peek lets us inspect the header without consuming it.
	header, err := f.reader.Peek(7)
	if err != nil {
		return nil, err
	}

	ft := FrameType(header[0])
	if !isValidFrameType(ft) {
		return nil, fmt.Errorf("invalid frame type %d", ft)
	}

	// Decode payload length from header bytes 3-6.
	length := int(uint32(header[3])<<24 | uint32(header[4])<<16 |
		uint32(header[5])<<8 | uint32(header[6]))
	if length > f.maxSize {
		return nil, fmt.Errorf(
			"frame payload %d exceeds max %d",
			length, f.maxSize,
		)
	}

	// Total frame on wire: 7 header + payload + 1 trailer.
	total := 7 + length + 1
	data, err := f.reader.Peek(total)
	if err != nil {
		return nil, err
	}
	if _, err := f.reader.Discard(total); err != nil {
		return nil, err
	}

	// Validate end-of-frame octet.
	if data[total-1] != FrameEnd {
		return nil, fmt.Errorf(
			"frame end 0x%02X, want 0x%02X",
			data[total-1], FrameEnd,
		)
	}

	// Decode channel from header bytes 1-2.
	ch := uint16(data[1])<<8 | uint16(data[2])
	payload := make([]byte, length)
	copy(payload, data[7:7+length])

	return &Frame{
		Type:    ft,
		Channel: ch,
		Payload: payload,
	}, nil
}

// isValidFrameType reports whether t is a recognised AMQP frame type.
func isValidFrameType(t FrameType) bool {
	switch t {
	case FrameMethod, FrameHeader, FrameBody, FrameHeartbeat:
		return true
	}
	return false
}
