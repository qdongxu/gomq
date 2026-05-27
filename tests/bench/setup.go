// setup.go — common helpers for the benchmark suite.
package bench

import (
	"fmt"

	"github.com/qdongxu/gomq/internal/server"
)

// benchServer returns a fully wired Server with a default direct
// exchange, queue and binding already declared.
func benchServer() *server.Server {
	srv := server.NewServer()
	return srv
}

// setupDirect creates a direct exchange, one queue and binds them.
func setupDirect(srv *server.Server, exName, qName, rk string) {
	ex := srv.ExchangeManager()
	qm := srv.QueueManager()
	bm := srv.BindingManager()
	_, _ = ex.Declare(exName, server.ExchangeDirect, false, false, false, nil)
	_, _ = qm.Declare(qName, false, false, false, nil, nil)
	_, _ = bm.Bind(exName, qName, rk, nil)
}

// setupFanout creates a fanout exchange and N queues bound to it.
func setupFanout(srv *server.Server, exName string, queues []string) {
	ex := srv.ExchangeManager()
	qm := srv.QueueManager()
	bm := srv.BindingManager()
	_, _ = ex.Declare(exName, server.ExchangeFanout, false, false, false, nil)
	for _, q := range queues {
		_, _ = qm.Declare(q, false, false, false, nil, nil)
		_, _ = bm.Bind(exName, q, "", nil)
	}
}

// setupTopic creates a topic exchange and binds queues with the
// given binding keys.
func setupTopic(srv *server.Server, exName string, bindings map[string]string) {
	ex := srv.ExchangeManager()
	qm := srv.QueueManager()
	bm := srv.BindingManager()
	_, _ = ex.Declare(exName, server.ExchangeTopic, false, false, false, nil)
	for qName, bindKey := range bindings {
		_, _ = qm.Declare(qName, false, false, false, nil, nil)
		_, _ = bm.Bind(exName, qName, bindKey, nil)
	}
}

// setupHeaders creates a headers exchange and binds a queue with the
// given header match table.
func setupHeaders(srv *server.Server, exName, qName string, headers map[string]interface{}) {
	ex := srv.ExchangeManager()
	qm := srv.QueueManager()
	bm := srv.BindingManager()
	_, _ = ex.Declare(exName, server.ExchangeHeaders, false, false, false, nil)
	_, _ = qm.Declare(qName, false, false, false, nil, nil)
	_, _ = bm.Bind(exName, qName, "", headers)
}

// benchMessage returns a Message whose payload is the requested size.
// The payload is filled with a deterministic pattern so compression
// does not skew results.
func benchMessage(size int) *server.Message {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i % 256)
	}
	return server.NewMessage(payload, server.Properties{})
}

// benchRoutingKey returns a deterministic routing key for benchmarks.
func benchRoutingKey(i int) string {
	return fmt.Sprintf("rk.%d", i%1000)
}
