// method_registry.go maps AMQP class/method IDs to handler functions.
package server

import (
	"fmt"
)

// MethodHandler processes an AMQP method frame for a channel.
type MethodHandler func(ch *Channel, payload []byte) error

// MethodRegistry stores handlers indexed by class+method ID.
type MethodRegistry interface {
	Register(classID, methodID uint16, handler MethodHandler)
	Lookup(classID, methodID uint16) (MethodHandler, bool)
}

// SimpleRegistry is a map-based MethodRegistry.
type SimpleRegistry struct {
	handlers map[uint32]MethodHandler
}

// NewSimpleRegistry creates an empty registry.
func NewSimpleRegistry() *SimpleRegistry {
	return &SimpleRegistry{
		handlers: make(map[uint32]MethodHandler),
	}
}

// Register adds a handler for the given class and method.
func (r *SimpleRegistry) Register(
	classID, methodID uint16,
	handler MethodHandler,
) {
	key := key(classID, methodID)
	r.handlers[key] = handler
}

// Lookup finds a handler by class and method ID.
func (r *SimpleRegistry) Lookup(
	classID, methodID uint16,
) (MethodHandler, bool) {
	h, ok := r.handlers[key(classID, methodID)]
	return h, ok
}

// key combines class and method IDs into a single lookup key.
func key(classID, methodID uint16) uint32 {
	return uint32(classID)<<16 | uint32(methodID)
}

// notImplementedHandler returns an error for unimplemented methods.
func notImplementedHandler(
	classID, methodID uint16,
) MethodHandler {
	return func(ch *Channel, payload []byte) error {
		return fmt.Errorf(
			"method %d.%d not implemented",
			classID, methodID,
		)
	}
}
