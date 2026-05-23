// zero_copy.go provides zero-copy utilities for message handling.
package server

// SharedSlice wraps a byte slice with reference counting to enable
// safe zero-copy sharing of payload data.
type SharedSlice struct {
	data []byte
	refs *int32
}

// NewSharedSlice creates a shared slice from a byte slice. The data
// is referenced, not copied.
func NewSharedSlice(data []byte) SharedSlice {
	refs := int32(1)
	return SharedSlice{data: data, refs: &refs}
}

// Clone creates another reference to the same underlying data.
// Callers must call Release when done.
func (s SharedSlice) Clone() SharedSlice {
	if s.refs != nil {
		*s.refs++
	}
	return SharedSlice{data: s.data, refs: s.refs}
}

// Bytes returns the underlying byte slice. The caller must not
// modify the returned slice.
func (s SharedSlice) Bytes() []byte {
	return s.data
}

// Len returns the length of the underlying data.
func (s SharedSlice) Len() int {
	return len(s.data)
}

// Release decrements the reference count. When the count reaches
// zero, the underlying data becomes eligible for GC.
func (s *SharedSlice) Release() {
	if s.refs != nil {
		*s.refs--
		if *s.refs <= 0 {
			s.data = nil
			s.refs = nil
		}
	}
}

// ZeroCopyMessage wraps Message with a shared payload to avoid deep
// copies when routing to multiple queues.
type ZeroCopyMessage struct {
	msg     *Message
	payload SharedSlice
}

// NewZeroCopyMessage creates a zero-copy wrapper for a message.
// The payload is referenced, not copied.
func NewZeroCopyMessage(msg *Message) *ZeroCopyMessage {
	return &ZeroCopyMessage{
		msg:     msg,
		payload: NewSharedSlice(msg.Payload()),
	}
}

// Message returns the wrapped message. The payload should be
// accessed via the SharedSlice to avoid copies.
func (z *ZeroCopyMessage) Message() *Message {
	return z.msg
}

// Payload returns the shared payload slice.
func (z *ZeroCopyMessage) Payload() SharedSlice {
	return z.payload.Clone()
}

// Release releases the shared payload reference.
func (z *ZeroCopyMessage) Release() {
	z.payload.Release()
}
