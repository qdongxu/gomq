// bridge.go connects the AMQP Server to the web UI.
package server

import (
	"github.com/qdongxu/gomq/internal/web"
)

// WebBroker implements web.Broker for the management UI.
type WebBroker struct {
	srv *Server
}

// NewWebBroker creates a broker state provider for the web UI.
func NewWebBroker(srv *Server) *WebBroker {
	return &WebBroker{srv: srv}
}

// ExchangeList returns all exchanges for the UI.
func (wb *WebBroker) ExchangeList() []web.ExchangeInfo {
	exs := wb.srv.ExchangeManager().List()
	out := make([]web.ExchangeInfo, 0, len(exs))
	for _, ex := range exs {
		inCnt, outCnt := wb.srv.Publisher().ExchangeStats(ex.Name)
		bindings := wb.srv.BindingManager().GetBindings(ex.Name)
		out = append(out, web.ExchangeInfo{
			Name:        ex.Name,
			Type:        string(ex.Type),
			Durable:     ex.Durable,
			Bindings:    len(bindings),
			MessagesIn:  inCnt,
			MessagesOut: outCnt,
		})
	}
	return out
}

// QueueList returns all queues for the UI.
func (wb *WebBroker) QueueList() []web.QueueInfo {
	qs := wb.srv.QueueManager().List()
	out := make([]web.QueueInfo, 0, len(qs))
	for _, q := range qs {
		out = append(out, web.QueueInfo{
			Name:      q.Name,
			Durable:   q.Durable,
			Messages:  wb.srv.MessageStore().Len(q.Name),
			Consumers: 0, // TODO: hook into ConsumerManager
		})
	}
	return out
}

// BindingList returns all bindings for the UI.
func (wb *WebBroker) BindingList() []web.BindingInfo {
	bs := wb.srv.BindingManager().ListAll()
	out := make([]web.BindingInfo, 0, len(bs))
	for _, b := range bs {
		out = append(out, web.BindingInfo{
			Exchange:   b.ExchangeName,
			Queue:      b.QueueName,
			RoutingKey: b.RoutingKey,
		})
	}
	return out
}

// ConnectionList returns all active connections for the UI.
func (wb *WebBroker) ConnectionList() []web.ConnectionInfo {
	conns := wb.srv.ConnectionList()
	out := make([]web.ConnectionInfo, 0, len(conns))
	for _, c := range conns {
		stateStr := "unknown"
		switch c.State() {
		case StateInit:
			stateStr = "init"
		case StateHeader:
			stateStr = "header"
		case StateStart:
			stateStr = "start"
		case StateTune:
			stateStr = "tune"
		case StateOpen:
			stateStr = "open"
		case StateClosing:
			stateStr = "closing"
		case StateClosed:
			stateStr = "closed"
		}
		out = append(out, web.ConnectionInfo{
			RemoteAddr: c.RemoteAddr(),
			State:      stateStr,
			Channels:   c.ChannelCount(),
			Heartbeat:  c.HeartbeatInterval(),
		})
	}
	return out
}

// ChannelList returns all active channels for the UI.
func (wb *WebBroker) ChannelList() []web.ChannelInfo {
	conns := wb.srv.ConnectionList()
	out := make([]web.ChannelInfo, 0)
	for _, c := range conns {
		if c.channelMgr == nil {
			continue
		}
		c.channelMgr.mu.RLock()
		chs := make([]*Channel, 0, len(c.channelMgr.channels))
		for _, ch := range c.channelMgr.channels {
			chs = append(chs, ch)
		}
		c.channelMgr.mu.RUnlock()

		for _, ch := range chs {
			stateStr := "unknown"
			switch ch.State() {
			case ChanOpening:
				stateStr = "opening"
			case ChanOpen:
				stateStr = "open"
			case ChanFlow:
				stateStr = "flow"
			case ChanClosing:
				stateStr = "closing"
			case ChanClosed:
				stateStr = "closed"
			}

			unacked := len(
				wb.srv.DeliveryTracker().GetUnacked(ch.ID()),
			)
			consumers := wb.srv.ConsumerManager().
				CountByChannel(ch)

			pref := ch.prefetch
			prefLimit := 0
			if pref != nil {
				l := pref.LimitFor(ch.ID())
				if l != nil {
					prefLimit = int(l.count)
				}
			}
			prefCount := 0
			if pref != nil {
				prefCount = int(pref.Current(ch.ID()))
			}

			out = append(out, web.ChannelInfo{
				ID:            ch.ID(),
				Connection:    c.RemoteAddr(),
				State:         stateStr,
				Consumers:     consumers,
				Unacked:       unacked,
				PrefetchCount: prefCount,
				PrefetchLimit: prefLimit,
			})
		}
	}
	return out
}
