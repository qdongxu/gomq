// Package amqp091 implements the AMQP 0-9-1 wire protocol framing
// and basic type encoding/decoding.
package amqp091

// FrameType identifies the kind of an AMQP frame.
type FrameType uint8

const (
	FrameMethod     FrameType = 1 // RPC method arguments
	FrameHeader     FrameType = 2 // Content header (properties)
	FrameBody       FrameType = 3 // Message body payload
	FrameHeartbeat  FrameType = 8 // Connection heartbeat
	FrameEnd        uint8     = 0xCE // End-of-frame octet
	FrameMinSize    int       = 4096 // Minimum frame size
	FrameMaxDefault int       = 131072 // Default max frame size
)

// Frame represents a single AMQP 0-9-1 wire frame.
type Frame struct {
	Type    FrameType // method, header, body, or heartbeat
	Channel uint16    // multiplexed channel number
	Payload []byte    // frame payload (may be empty)
}

// Size returns the total wire size of the frame including header
// and trailer.
func (f *Frame) Size() int {
	return 1 + 2 + 4 + len(f.Payload) + 1
}

// IsValidType reports whether the frame type is one of the four
// recognised AMQP frame types.
func (f *Frame) IsValidType() bool {
	switch f.Type {
	case FrameMethod, FrameHeader, FrameBody, FrameHeartbeat:
		return true
	}
	return false
}
