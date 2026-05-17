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
		out = append(out, web.ExchangeInfo{
			Name: ex.Name,
			Type: string(ex.Type),
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
