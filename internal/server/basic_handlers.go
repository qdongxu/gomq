// basic_handlers.go implements AMQP Basic class method handlers.
package server

import (
	"bytes"

	"github.com/qdongxu/gomq/pkg/protocol/amqp091"
)

// RegisterBasicHandlers registers Basic class method handlers.
func RegisterBasicHandlers(
	reg *SimpleRegistry,
	srv *Server,
) {
	reg.Register(60, 20, handleConsume(srv))
	reg.Register(60, 30, handleCancel(srv))
}

// handleConsume decodes Basic.Consume and subscribes a consumer.
func handleConsume(srv *Server) MethodHandler {
	return func(ch *Channel, payload []byte) error {
		dec := amqp091.NewDecoder(bytes.NewReader(payload))

		// reserved-1 (short)
		_, _ = dec.ReadUint16()

		queue, err := dec.ReadShortString()
		if err != nil {
			return err
		}

		tag, err := dec.ReadShortString()
		if err != nil {
			return err
		}

		bits, err := dec.ReadUint8()
		if err != nil {
			return err
		}
		_ = bits&0x01 != 0 // no-local not used
		noAck := bits&0x02 != 0
		exclusive := bits&0x04 != 0
		noWait := bits&0x08 != 0

		args, err := dec.ReadTable()
		if err != nil {
			return err
		}

		_, err = srv.ConsumerManager().Subscribe(
			tag, queue, ch, noAck, exclusive, args,
		)
		if err != nil {
			return err
		}

		if !noWait {
			enc := amqp091.NewEncoder()
			_ = enc.WriteUint16(60) // class
			_ = enc.WriteUint16(21) // ConsumeOk
			_ = enc.WriteShortString(tag)
			_ = ch.SendFrame(&amqp091.Frame{
				Type:    amqp091.FrameMethod,
				Payload: enc.Bytes(),
			})
		}
		return nil
	}
}

// handleCancel decodes Basic.Cancel and unsubscribes a consumer.
func handleCancel(srv *Server) MethodHandler {
	return func(ch *Channel, payload []byte) error {
		dec := amqp091.NewDecoder(bytes.NewReader(payload))

		tag, err := dec.ReadShortString()
		if err != nil {
			return err
		}

		bits, err := dec.ReadUint8()
		if err != nil {
			return err
		}
		noWait := bits&0x01 != 0

		_ = srv.ConsumerManager().Unsubscribe(tag)

		if !noWait {
			enc := amqp091.NewEncoder()
			_ = enc.WriteUint16(60) // class
			_ = enc.WriteUint16(31) // CancelOk
			_ = enc.WriteShortString(tag)
			_ = ch.SendFrame(&amqp091.Frame{
				Type:    amqp091.FrameMethod,
				Payload: enc.Bytes(),
			})
		}
		return nil
	}
}
