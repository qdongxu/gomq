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
	reg.Register(60, 40, handlePublish(srv))
	reg.Register(60, 70, handleGet(srv))
	reg.Register(60, 80, handleAck(srv))
	reg.Register(60, 90, handleReject(srv))
	reg.Register(60, 120, handleNack(srv))
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
			_ = ch.SendFrame(
				&amqp091.Frame{
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
			_ = ch.SendFrame(
				&amqp091.Frame{
					Type:    amqp091.FrameMethod,
					Payload: enc.Bytes(),
				})
		}
		return nil
	}
}

// handlePublish decodes Basic.Publish and routes the message.
func handlePublish(srv *Server) MethodHandler {
	return func(ch *Channel, payload []byte) error {
		dec := amqp091.NewDecoder(bytes.NewReader(payload))

		// reserved-1 (short)
		_, _ = dec.ReadUint16()

		exchange, err := dec.ReadShortString()
		if err != nil {
			return err
		}

		routingKey, err := dec.ReadShortString()
		if err != nil {
			return err
		}

		bits, err := dec.ReadUint8()
		if err != nil {
			return err
		}
		_ = bits&0x01 != 0 // mandatory
		_ = bits&0x02 != 0 // immediate

		msg := NewMessage(nil, Properties{})
		msg.SetRoutingMeta(exchange, routingKey)
		return srv.Publisher().Publish(
			exchange, routingKey, msg, ch.ID(),
		)
	}
}

// handleGet decodes Basic.Get and fetches one message from a queue.
func handleGet(srv *Server) MethodHandler {
	return func(ch *Channel, payload []byte) error {
		dec := amqp091.NewDecoder(bytes.NewReader(payload))

		// reserved-1 (short)
		_, _ = dec.ReadUint16()

		queue, err := dec.ReadShortString()
		if err != nil {
			return err
		}

		bits, err := dec.ReadUint8()
		if err != nil {
			return err
		}
		noAck := bits&0x01 != 0
		_ = bits&0x02 != 0 // no-wait not used for get

		pc := NewPullConsumer(srv.MessageStore(), srv.DeliveryTracker())
		msg, ok := pc.Get(queue, noAck, ch.ID())
		if !ok {
			// send Basic.GetEmpty when no message available
			enc := amqp091.NewEncoder()
			_ = enc.WriteUint16(60) // class
			_ = enc.WriteUint16(72) // GetEmpty
			_ = enc.WriteUint16(0)  // reserved-1
			_ = ch.SendFrame(
				&amqp091.Frame{
					Type:    amqp091.FrameMethod,
					Payload: enc.Bytes(),
				})
			return nil
		}

		// send Basic.GetOk
		enc := amqp091.NewEncoder()
		_ = enc.WriteUint16(60) // class
		_ = enc.WriteUint16(71) // GetOk
		_ = enc.WriteUint64(msg.DeliveryTag())
		_ = enc.WriteBool(false) // redelivered
		_ = enc.WriteShortString(queue)
		_ = enc.WriteUint16(0) // reserved-1
		_ = ch.SendFrame(
			&amqp091.Frame{
				Type:    amqp091.FrameMethod,
				Payload: enc.Bytes(),
			})
		return nil
	}
}

// handleAck decodes Basic.Ack and confirms a delivery.
func handleAck(srv *Server) MethodHandler {
	return func(ch *Channel, payload []byte) error {
		dec := amqp091.NewDecoder(bytes.NewReader(payload))

		tag, err := dec.ReadUint64()
		if err != nil {
			return err
		}

		_ = srv.DeliveryTracker().Ack(tag, ch.ID())
		return nil
	}
}

// handleNack decodes Basic.Nack and rejects a delivery.
func handleNack(srv *Server) MethodHandler {
	return func(ch *Channel, payload []byte) error {
		dec := amqp091.NewDecoder(bytes.NewReader(payload))

		tag, err := dec.ReadUint64()
		if err != nil {
			return err
		}

		bits, err := dec.ReadUint8()
		if err != nil {
			return err
		}
		requeue := bits&0x01 != 0

		_ = srv.DeliveryTracker().Nack(tag, ch.ID(), requeue)
		return nil
	}
}

// handleReject decodes Basic.Reject and rejects a single delivery.
func handleReject(srv *Server) MethodHandler {
	return func(ch *Channel, payload []byte) error {
		dec := amqp091.NewDecoder(bytes.NewReader(payload))

		tag, err := dec.ReadUint64()
		if err != nil {
			return err
		}

		bits, err := dec.ReadUint8()
		if err != nil {
			return err
		}
		requeue := bits&0x01 != 0

		_ = srv.DeliveryTracker().Reject(tag, ch.ID(), requeue)
		return nil
	}
}
