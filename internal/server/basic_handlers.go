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
	reg.Register(10, 50, handleConnectionClose(srv))
	reg.Register(20, 20, handleChannelFlow(srv))
	reg.Register(20, 40, handleChannelClose(srv))
	reg.Register(60, 10, handleQos(srv))
	reg.Register(60, 20, handleConsume(srv))
	reg.Register(60, 30, handleCancel(srv))
	reg.Register(60, 40, handlePublish(srv))
	reg.Register(60, 70, handleGet(srv))
	reg.Register(60, 80, handleAck(srv))
	reg.Register(60, 90, handleReject(srv))
	reg.Register(60, 110, handleRecover(srv))
	reg.Register(60, 120, handleNack(srv))
	reg.Register(50, 10, handleQueueDeclare(srv))
	reg.Register(50, 20, handleQueueBind(srv))
	reg.Register(50, 30, handleQueuePurge(srv))
	reg.Register(50, 40, handleQueueDelete(srv))
	reg.Register(50, 50, handleQueueUnbind(srv))
	reg.Register(40, 10, handleExchangeDeclare(srv))
	reg.Register(40, 20, handleExchangeDelete(srv))
	reg.Register(85, 10, handleConfirmSelect(srv))
	reg.Register(90, 10, handleTxSelect(srv))
	reg.Register(90, 20, handleTxCommit(srv))
	reg.Register(90, 30, handleTxRollback(srv))
}

// handleQos decodes Basic.Qos and updates the channel prefetch limit.
func handleQos(srv *Server) MethodHandler {
	return func(ch *Channel, payload []byte) error {
		dec := amqp091.NewDecoder(bytes.NewReader(payload))

		prefetchSize, err := dec.ReadUint32()
		if err != nil {
			return err
		}

		prefetchCount, err := dec.ReadUint16()
		if err != nil {
			return err
		}

		bits, err := dec.ReadUint8()
		if err != nil {
			return err
		}
		global := bits&0x01 != 0

		srv.Prefetch().SetChannelPrefetch(
			ch.ID(), prefetchCount, uint16(prefetchSize), global,
		)

		enc := amqp091.NewEncoder()
		_ = enc.WriteUint16(60) // class
		_ = enc.WriteUint16(11) // QosOk
		_ = ch.SendFrame(
			&amqp091.Frame{
				Type:    amqp091.FrameMethod,
				Payload: enc.Bytes(),
			})
		return nil
	}
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
		mandatory := bits&0x01 != 0
		_ = bits&0x02 != 0 // immediate

		msg := NewMessage(nil, Properties{})
		msg.SetRoutingMeta(exchange, routingKey)

		var routed int
		if ch.IsTxMode() {
			ch.StageTxPublish(exchange, routingKey, msg)
			routed = 0 // will route on commit
		} else if ch.IsConfirmMode() {
			tag := ch.NextDeliveryTag()
			routed, err = srv.Publisher().Publish(
				exchange, routingKey, msg, ch.ID(),
			)
			if err != nil {
				return err
			}
			sendPublisherConfirm(ch, tag, routed > 0)
		} else {
			routed, err = srv.Publisher().Publish(
				exchange, routingKey, msg, ch.ID(),
			)
			if err != nil {
				return err
			}
		}

		if mandatory && routed == 0 {
			enc := amqp091.NewEncoder()
			_ = enc.WriteUint16(60)  // class
			_ = enc.WriteUint16(50)  // Return
			_ = enc.WriteUint16(312) // reply-code NO_ROUTE
			_ = enc.WriteShortString("NO_ROUTE")
			_ = enc.WriteShortString(exchange)
			_ = enc.WriteShortString(routingKey)
			_ = ch.SendFrame(
				&amqp091.Frame{
					Type:    amqp091.FrameMethod,
					Payload: enc.Bytes(),
				})
		}
		return nil
	}
}

// sendPublisherConfirm sends Basic.Ack or Basic.Nack to the
// publisher on the given channel based on whether the message
// was routed to any queue.
func sendPublisherConfirm(ch *Channel, tag uint64, ack bool) {
	enc := amqp091.NewEncoder()
	_ = enc.WriteUint16(60) // class Basic
	if ack {
		_ = enc.WriteUint16(80) // Ack
		_ = enc.WriteUint64(tag)
		_ = enc.WriteUint8(0) // multiple=false
	} else {
		_ = enc.WriteUint16(120) // Nack
		_ = enc.WriteUint64(tag)
		_ = enc.WriteUint8(0) // multiple=false, requeue=false
	}
	_ = ch.SendFrame(
		&amqp091.Frame{
			Type:    amqp091.FrameMethod,
			Payload: enc.Bytes(),
		})
}

// handleConfirmSelect decodes Confirm.Select and enables publisher
// confirm mode on the channel.
func handleConfirmSelect(srv *Server) MethodHandler {
	return func(ch *Channel, payload []byte) error {
		dec := amqp091.NewDecoder(bytes.NewReader(payload))

		bits, err := dec.ReadUint8()
		if err != nil {
			return err
		}
		noWait := bits&0x01 != 0

		ch.SetConfirmMode()

		if !noWait {
			enc := amqp091.NewEncoder()
			_ = enc.WriteUint16(85)   // class Confirm
			_ = enc.WriteUint16(11)   // SelectOk
			_ = ch.SendFrame(
				&amqp091.Frame{
					Type:    amqp091.FrameMethod,
					Payload: enc.Bytes(),
				})
		}
		return nil
	}
}

// handleTxSelect decodes Tx.Select and enables transaction mode
// on the channel. Returns precondition-failed if confirm mode is
// already active.
func handleTxSelect(srv *Server) MethodHandler {
	return func(ch *Channel, payload []byte) error {
		// no arguments
		if ch.IsConfirmMode() {
			return sendChannelClose(ch, 406, "PRECONDITION_FAILED",
				"transactions and publisher confirms are mutually exclusive")
		}
		ch.SetTxMode()
		enc := amqp091.NewEncoder()
		_ = enc.WriteUint16(90)   // class Tx
		_ = enc.WriteUint16(11)   // SelectOk
		_ = ch.SendFrame(
			&amqp091.Frame{
				Type:    amqp091.FrameMethod,
				Payload: enc.Bytes(),
			})
		return nil
	}
}

// handleTxCommit decodes Tx.Commit and flushes all staged
// messages on the channel.
func handleTxCommit(srv *Server) MethodHandler {
	return func(ch *Channel, payload []byte) error {
		// no arguments
		entries := ch.CommitTx()
		var lastRouted int
		for _, e := range entries {
			routed, err := srv.Publisher().Publish(
				e.exchange, e.routingKey, e.msg, ch.ID(),
			)
			if err != nil {
				return err
			}
			lastRouted = routed
		}
		_ = lastRouted
		enc := amqp091.NewEncoder()
		_ = enc.WriteUint16(90)   // class Tx
		_ = enc.WriteUint16(21)   // CommitOk
		_ = ch.SendFrame(
			&amqp091.Frame{
				Type:    amqp091.FrameMethod,
				Payload: enc.Bytes(),
			})
		return nil
	}
}

// handleTxRollback decodes Tx.Rollback and discards all staged
// messages on the channel.
func handleTxRollback(srv *Server) MethodHandler {
	return func(ch *Channel, payload []byte) error {
		// no arguments
		ch.RollbackTx()
		enc := amqp091.NewEncoder()
		_ = enc.WriteUint16(90)   // class Tx
		_ = enc.WriteUint16(31)   // RollbackOk
		_ = ch.SendFrame(
			&amqp091.Frame{
				Type:    amqp091.FrameMethod,
				Payload: enc.Bytes(),
			})
		return nil
	}
}

// sendChannelClose builds and sends a Channel.Close frame with
// the given reply code and text, then closes the channel.
func sendChannelClose(ch *Channel, code uint16, text, detail string) error {
	enc := amqp091.NewEncoder()
	_ = enc.WriteUint16(20) // class Channel
	_ = enc.WriteUint16(40) // Close
	_ = enc.WriteUint16(code)
	_ = enc.WriteShortString(text)
	_ = enc.WriteShortString(detail)
	_ = enc.WriteUint16(0) // class-id
	_ = enc.WriteUint16(0) // method-id
	err := ch.SendFrame(
		&amqp091.Frame{
			Type:    amqp091.FrameMethod,
			Payload: enc.Bytes(),
		})
	ch.Close()
	return err
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

// handleRecover decodes Basic.Recover and recovers all unacknowledged
// deliveries for the channel. When requeue is true, messages are
// returned to their original queues; otherwise they are discarded.
func handleRecover(srv *Server) MethodHandler {
	return func(ch *Channel, payload []byte) error {
		dec := amqp091.NewDecoder(bytes.NewReader(payload))

		bits, err := dec.ReadUint8()
		if err != nil {
			return err
		}
		requeue := bits&0x01 != 0

		_ = srv.DeliveryTracker().RecoverAll(ch.ID(), requeue)

		enc := amqp091.NewEncoder()
		_ = enc.WriteUint16(60)  // class
		_ = enc.WriteUint16(111) // RecoverOk
		_ = ch.SendFrame(
			&amqp091.Frame{
				Type:    amqp091.FrameMethod,
				Payload: enc.Bytes(),
			})
		return nil
	}
}

// handleQueueDeclare decodes Queue.Declare and creates/looks up a queue.
func handleQueueDeclare(srv *Server) MethodHandler {
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
		passive := bits&0x01 != 0
		durable := bits&0x02 != 0
		exclusive := bits&0x04 != 0
		autoDelete := bits&0x08 != 0
		noWait := bits&0x10 != 0

		args, err := dec.ReadTable()
		if err != nil {
			return err
		}

		var q *Queue
		if passive {
			var ok bool
			q, ok = srv.QueueManager().Get(queue)
			if !ok {
				return nil
			}
		} else {
			q, _ = srv.QueueManager().Declare(
				queue, durable, exclusive, autoDelete, args, nil,
			)
		}

		if !noWait && q != nil {
			enc := amqp091.NewEncoder()
			_ = enc.WriteUint16(50) // class
			_ = enc.WriteUint16(11) // DeclareOk
			_ = enc.WriteShortString(q.Name)
			_ = enc.WriteUint32(0)  // message-count
			_ = enc.WriteUint32(0)  // consumer-count
			_ = ch.SendFrame(
				&amqp091.Frame{
					Type:    amqp091.FrameMethod,
					Payload: enc.Bytes(),
				})
		}
		return nil
	}
}

// handleQueueBind decodes Queue.Bind and creates a binding.
func handleQueueBind(srv *Server) MethodHandler {
	return func(ch *Channel, payload []byte) error {
		dec := amqp091.NewDecoder(bytes.NewReader(payload))

		// reserved-1 (short)
		_, _ = dec.ReadUint16()

		queue, err := dec.ReadShortString()
		if err != nil {
			return err
		}

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
		noWait := bits&0x01 != 0

		args, err := dec.ReadTable()
		if err != nil {
			return err
		}

		srv.BindingManager().Bind(exchange, queue, routingKey, args)

		if !noWait {
			enc := amqp091.NewEncoder()
			_ = enc.WriteUint16(50) // class
			_ = enc.WriteUint16(21) // BindOk
			_ = ch.SendFrame(
				&amqp091.Frame{
					Type:    amqp091.FrameMethod,
					Payload: enc.Bytes(),
				})
		}
		return nil
	}
}

// handleQueueDelete decodes Queue.Delete and removes a queue.
func handleQueueDelete(srv *Server) MethodHandler {
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
		_ = bits&0x01 != 0 // if-unused not enforced
		_ = bits&0x02 != 0 // if-empty not enforced
		noWait := bits&0x04 != 0

		srv.QueueManager().Delete(queue)

		if !noWait {
			enc := amqp091.NewEncoder()
			_ = enc.WriteUint16(50) // class
			_ = enc.WriteUint16(41) // DeleteOk
			_ = enc.WriteUint32(0)  // message-count
			_ = ch.SendFrame(
				&amqp091.Frame{
					Type:    amqp091.FrameMethod,
					Payload: enc.Bytes(),
				})
		}
		return nil
	}
}

// handleQueuePurge decodes Queue.Purge and removes all
// messages from a queue.
func handleQueuePurge(srv *Server) MethodHandler {
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
		noWait := bits&0x01 != 0

		count := uint32(srv.MessageStore().Len(queue))
		srv.MessageStore().Purge(queue)

		if !noWait {
			enc := amqp091.NewEncoder()
			_ = enc.WriteUint16(50) // class
			_ = enc.WriteUint16(31) // PurgeOk
			_ = enc.WriteUint32(count)
			_ = ch.SendFrame(
				&amqp091.Frame{
					Type:    amqp091.FrameMethod,
					Payload: enc.Bytes(),
				})
		}
		return nil
	}
}

// handleQueueUnbind decodes Queue.Unbind and removes a binding.
func handleQueueUnbind(srv *Server) MethodHandler {
	return func(ch *Channel, payload []byte) error {
		dec := amqp091.NewDecoder(bytes.NewReader(payload))

		// reserved-1 (short)
		_, _ = dec.ReadUint16()

		queue, err := dec.ReadShortString()
		if err != nil {
			return err
		}

		exchange, err := dec.ReadShortString()
		if err != nil {
			return err
		}

		routingKey, err := dec.ReadShortString()
		if err != nil {
			return err
		}

		_, err = dec.ReadTable()
		if err != nil {
			return err
		}

		srv.BindingManager().Unbind(exchange, queue, routingKey)

		enc := amqp091.NewEncoder()
		_ = enc.WriteUint16(50) // class
		_ = enc.WriteUint16(51) // UnbindOk
		_ = ch.SendFrame(
			&amqp091.Frame{
				Type:    amqp091.FrameMethod,
				Payload: enc.Bytes(),
			})
		return nil
	}
}

// handleExchangeDeclare decodes Exchange.Declare and
// creates/looks up an exchange.
func handleExchangeDeclare(srv *Server) MethodHandler {
	return func(ch *Channel, payload []byte) error {
		dec := amqp091.NewDecoder(bytes.NewReader(payload))

		// reserved-1 (short)
		_, _ = dec.ReadUint16()

		exchange, err := dec.ReadShortString()
		if err != nil {
			return err
		}

		exType, err := dec.ReadShortString()
		if err != nil {
			return err
		}

		bits, err := dec.ReadUint8()
		if err != nil {
			return err
		}
		passive := bits&0x01 != 0
		_ = bits&0x02 != 0 // durable not enforced
		_ = bits&0x04 != 0 // auto-delete not enforced
		_ = bits&0x08 != 0 // internal not enforced
		noWait := bits&0x10 != 0

		args, err := dec.ReadTable()
		if err != nil {
			return err
		}

		var et ExchangeType
		switch exType {
		case "direct":
			et = ExchangeDirect
		case "fanout":
			et = ExchangeFanout
		default:
			et = ExchangeDirect
		}

		var ex *Exchange
		if passive {
			var ok bool
			ex, ok = srv.ExchangeManager().Get(exchange)
			if !ok {
				return nil
			}
		} else {
			ex, _ = srv.ExchangeManager().Declare(
				exchange, et, false, false, false, args,
			)
		}

		if !noWait && ex != nil {
			enc := amqp091.NewEncoder()
			_ = enc.WriteUint16(40) // class
			_ = enc.WriteUint16(11) // DeclareOk
			_ = ch.SendFrame(
				&amqp091.Frame{
					Type:    amqp091.FrameMethod,
					Payload: enc.Bytes(),
				})
		}
		return nil
	}
}

// handleExchangeDelete decodes Exchange.Delete and removes an exchange.
func handleExchangeDelete(srv *Server) MethodHandler {
	return func(ch *Channel, payload []byte) error {
		dec := amqp091.NewDecoder(bytes.NewReader(payload))

		// reserved-1 (short)
		_, _ = dec.ReadUint16()

		exchange, err := dec.ReadShortString()
		if err != nil {
			return err
		}

		bits, err := dec.ReadUint8()
		if err != nil {
			return err
		}
		_ = bits&0x01 != 0 // if-unused not enforced
		noWait := bits&0x02 != 0

		srv.ExchangeManager().Delete(exchange)

		if !noWait {
			enc := amqp091.NewEncoder()
			_ = enc.WriteUint16(40) // class
			_ = enc.WriteUint16(21) // DeleteOk
			_ = ch.SendFrame(
				&amqp091.Frame{
					Type:    amqp091.FrameMethod,
					Payload: enc.Bytes(),
				})
		}
		return nil
	}
}

// handleConnectionClose decodes Connection.Close and
// closes the connection.
func handleConnectionClose(srv *Server) MethodHandler {
	return func(ch *Channel, payload []byte) error {
		dec := amqp091.NewDecoder(bytes.NewReader(payload))

		// reply-code (short)
		_, _ = dec.ReadUint16()

		// reply-text (shortstr)
		_, _ = dec.ReadShortString()

		// class-id (short)
		_, _ = dec.ReadUint16()

		// method-id (short)
		_, _ = dec.ReadUint16()

		// Send Connection.CloseOk
		enc := amqp091.NewEncoder()
		_ = enc.WriteUint16(10) // class
		_ = enc.WriteUint16(51) // CloseOk
		_ = ch.SendFrame(
			&amqp091.Frame{
				Type:    amqp091.FrameMethod,
				Payload: enc.Bytes(),
			})

		// Close the underlying connection
		_ = ch.Conn().Close()
		return nil
	}
}

// handleChannelFlow decodes Channel.Flow and toggles channel flow state.
func handleChannelFlow(srv *Server) MethodHandler {
	return func(ch *Channel, payload []byte) error {
		dec := amqp091.NewDecoder(bytes.NewReader(payload))

		bits, err := dec.ReadUint8()
		if err != nil {
			return err
		}
		active := bits&0x01 != 0

		if active {
			ch.SetFlow(true)
			srv.FlowController().ResumeChannel(ch.ID())
		} else {
			ch.SetFlow(false)
			srv.FlowController().PauseChannel(ch.ID())
		}

		enc := amqp091.NewEncoder()
		_ = enc.WriteUint16(20) // class
		_ = enc.WriteUint16(21) // FlowOk
		var replyBits uint8
		if active {
			replyBits |= 0x01
		}
		_ = enc.WriteUint8(replyBits)
		_ = ch.SendFrame(
			&amqp091.Frame{
				Type:    amqp091.FrameMethod,
				Payload: enc.Bytes(),
			})
		return nil
	}
}

// handleChannelClose decodes Channel.Close and closes the channel.
func handleChannelClose(srv *Server) MethodHandler {
	return func(ch *Channel, payload []byte) error {
		dec := amqp091.NewDecoder(bytes.NewReader(payload))

		// reply-code (short)
		_, _ = dec.ReadUint16()

		// reply-text (shortstr)
		_, _ = dec.ReadShortString()

		// class-id (short)
		_, _ = dec.ReadUint16()

		// method-id (short)
		_, _ = dec.ReadUint16()

		// Send Channel.CloseOk
		enc := amqp091.NewEncoder()
		_ = enc.WriteUint16(20) // class
		_ = enc.WriteUint16(41) // CloseOk
		_ = ch.SendFrame(
			&amqp091.Frame{
				Type:    amqp091.FrameMethod,
				Payload: enc.Bytes(),
			})

		// Close the channel
		ch.Close()
		return nil
	}
}
