package server

import (
	"github.com/qdongxu/gomq/internal/web"
)

// Overview returns node-level summary data for the web UI.
func (wb *WebBroker) Overview() web.OverviewInfo {
	srv := wb.srv

	// Count connections.
	conns := srv.ConnectionList()
	connCount := len(conns)

	// Count channels across all connections.
	channelCount := 0
	for _, c := range conns {
		channelCount += c.ChannelCount()
	}

	// Count exchanges, queues, consumers.
	exCount := srv.ExchangeManager().Count()
	qCount := srv.QueueManager().Count()
	consumerCount := srv.ConsumerManager().Count()

	// Sum messages across all queues.
	msgCount := 0
	for _, q := range srv.QueueManager().List() {
		msgCount += srv.MessageStore().Len(q.Name)
	}

	return web.OverviewInfo{
		Connections: connCount,
		Channels:    channelCount,
		Exchanges:   exCount,
		Queues:      qCount,
		Consumers:   consumerCount,
		Messages:    msgCount,
	}
}
