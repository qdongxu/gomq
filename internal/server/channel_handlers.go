// channel_handlers.go implements AMQP Channel class method handlers.
package server

import (
	"bytes"

	"github.com/qdongxu/gomq/pkg/protocol/amqp091"
)

// RegisterChannelHandlers registers Channel class method handlers.
func RegisterChannelHandlers(
	reg *SimpleRegistry,
	srv *Server,
) {
	reg.Register(20, 10, handleChannelOpen(srv))
	reg.Register(20, 100, handleChannelRecover(srv))
}

// handleChannelOpen decodes Channel.Open and transitions the
// channel to Open state, sending Channel.OpenOk.
func handleChannelOpen(srv *Server) MethodHandler {
	return func(ch *Channel, payload []byte) error {
		// Channel.Open has no arguments in AMQP 0-9-1.
		ch.Open()

		enc := amqp091.NewEncoder()
		_ = enc.WriteUint16(20) // class Channel
		_ = enc.WriteUint16(11) // OpenOk
		_ = ch.SendFrame(
			&amqp091.Frame{
				Type:    amqp091.FrameMethod,
				Payload: enc.Bytes(),
			})
		return nil
	}
}

// handleChannelRecover decodes Channel.Recover and marks all
// unacknowledged deliveries for the channel as redeliverable.
// When requeue is true, messages are re-enqueued; otherwise they
// are simply cleared from tracking so the broker will redeliver.
func handleChannelRecover(srv *Server) MethodHandler {
	return func(ch *Channel, payload []byte) error {
		dec := amqp091.NewDecoder(bytes.NewReader(payload))

		bits, err := dec.ReadUint8()
		if err != nil {
			return err
		}
		requeue := bits&0x01 != 0

		// Recover all unacknowledged deliveries for this channel.
		_ = srv.DeliveryTracker().RecoverAll(ch.ID(), requeue)

		enc := amqp091.NewEncoder()
		_ = enc.WriteUint16(20)  // class Channel
		_ = enc.WriteUint16(101) // RecoverOk
		_ = ch.SendFrame(
			&amqp091.Frame{
				Type:    amqp091.FrameMethod,
				Payload: enc.Bytes(),
			})
		return nil
	}
}
