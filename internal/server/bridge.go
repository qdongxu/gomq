// bridge.go connects the AMQP Server to the web UI.
package server

import (
	"time"

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
		consumers := len(
			wb.srv.ConsumerManager().GetConsumers(q.Name),
		)
		bindings := wb.srv.BindingManager().
			GetBindingsForQueue(q.Name)
		out = append(out, web.QueueInfo{
			Name:      q.Name,
			Durable:   q.Durable,
			Messages:  wb.srv.MessageStore().Len(q.Name),
			Consumers: consumers,
			Bindings:  len(bindings),
			Memory:    wb.srv.MessageStore().Bytes(q.Name),
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

// MessageList returns paginated messages from a queue for the UI.
func (wb *WebBroker) MessageList(queueName string, limit, offset int) []web.MessageInfo {
	msgs := wb.srv.MessageStore().MessageList(queueName, limit, offset)
	out := make([]web.MessageInfo, 0, len(msgs))
	for _, m := range msgs {
		payload := string(m.Payload())
		if len(payload) > 200 {
			payload = payload[:200] + "..."
		}
		out = append(out, web.MessageInfo{
			DeliveryTag: m.DeliveryTag(),
			Payload:     payload,
			Headers:     m.Properties().Headers,
			Timestamp:   m.EnqueuedAt().Format(time.RFC3339),
			Exchange:    m.Exchange(),
			RoutingKey:  m.RoutingKey(),
		})
	}
	return out
}

const serverVersion = "0.1.0"

// Admin returns server admin data for the UI.
func (wb *WebBroker) Admin() web.AdminInfo {
	uptime := time.Since(wb.srv.StartTime())
	uptimeStr := ""
	if uptime < time.Minute {
		uptimeStr = uptime.Round(time.Second).String()
	} else if uptime < time.Hour {
		uptimeStr = uptime.Round(time.Minute).String()
	} else {
		uptimeStr = uptime.Round(time.Minute).String()
	}

	vhosts := []web.VHostInfo{
		{
			Name:      "/",
			Exchanges: len(wb.srv.ExchangeManager().List()),
			Queues:    len(wb.srv.QueueManager().List()),
		},
	}

	// Users are not yet configurable via TOML; Auth struct is
	// intentionally empty. Return empty list until config-driven
	// authentication is implemented.
	users := []web.UserInfo{}

	return web.AdminInfo{
		Version: serverVersion,
		Uptime:  uptimeStr,
		VHosts:  vhosts,
		Users:   users,
	}
}

// ClusterList returns all known cluster nodes for the UI.
func (wb *WebBroker) ClusterList() []web.ClusterNodeInfo {
	membership := wb.srv.Membership()
	cluster := wb.srv.Cluster()
	raftNode := wb.srv.RaftNode()
	if membership == nil {
		return []web.ClusterNodeInfo{}
	}

	members := membership.Members()
	out := make([]web.ClusterNodeInfo, 0, len(members))

	var leaderID string
	if cluster != nil {
		leaderID = cluster.Leader()
	}

	var localID string
	if cluster != nil {
		localID = cluster.LocalID()
	}

	// Aggregate local message stats.
	var localMsgIn, localMsgOut int64
	for _, ex := range wb.srv.ExchangeManager().List() {
		inCnt, outCnt := wb.srv.Publisher().ExchangeStats(ex.Name)
		localMsgIn += inCnt
		localMsgOut += outCnt
	}

	for _, m := range members {
		role := "follower"
		if m.ID == leaderID {
			role = "leader"
		}

		uptime := "unknown"
		conns := 0
		msgIn := int64(0)
		msgOut := int64(0)
		logIndex := uint64(0)
		logTerm := uint64(0)
		heartbeat := "unknown"

		if m.ID == localID {
			uptime = clusterUptimeSince(wb.srv.StartTime())
			conns = wb.srv.ConnectionCount()
			msgIn = localMsgIn
			msgOut = localMsgOut
			if raftNode != nil {
				logIndex = raftNode.CommitIndex()
				logTerm = raftNode.Term()
			}
			heartbeat = "0ms" // local node, no latency
		}

		out = append(out, web.ClusterNodeInfo{
			ID:        m.ID,
			Addr:      m.Addr,
			Status:    m.Status.String(),
			Role:      role,
			Uptime:    uptime,
			Conns:     conns,
			MsgIn:     msgIn,
			MsgOut:    msgOut,
			LogIndex:  logIndex,
			LogTerm:   logTerm,
			Heartbeat: heartbeat,
		})
	}
	return out
}

// clusterUptimeSince formats duration as human-readable string.
func clusterUptimeSince(t time.Time) string {
	d := time.Since(t)
	if d < time.Minute {
		return d.Round(time.Second).String()
	}
	if d < time.Hour {
		return d.Round(time.Minute).String()
	}
	return d.Round(time.Minute).String()
}
