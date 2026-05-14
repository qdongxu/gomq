// dispatcher.go routes incoming method frames to registered handlers.
package server

import (
	"bytes"
	"fmt"

	"github.com/qdongxu/gomq/pkg/protocol/amqp091"
)

// Dispatcher routes AMQP method frames to the appropriate handler.
type Dispatcher struct {
	registry MethodRegistry
}

// NewDispatcher creates a dispatcher with the given registry.
func NewDispatcher(reg MethodRegistry) *Dispatcher {
	return &Dispatcher{registry: reg}
}

// Dispatch decodes a method frame and invokes its handler.
func (d *Dispatcher) Dispatch(ch *Channel, f *amqp091.Frame) error {
	if f.Type != amqp091.FrameMethod {
		return fmt.Errorf("expected method frame, got %d", f.Type)
	}
	dec := amqp091.NewDecoder(bytes.NewReader(f.Payload))
	classID, err := dec.ReadUint16()
	if err != nil {
		return fmt.Errorf("read class-id: %w", err)
	}
	methodID, err := dec.ReadUint16()
	if err != nil {
		return fmt.Errorf("read method-id: %w", err)
	}
	handler, ok := d.registry.Lookup(classID, methodID)
	if !ok {
		return fmt.Errorf(
			"no handler for %d.%d", classID, methodID,
		)
	}
	return handler(ch, f.Payload)
}
