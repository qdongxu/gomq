// topic_routing_test.go — Topic exchange wildcard routing integration.
package integration

import (
	"bytes"
	"testing"

	"github.com/qdongxu/gomq/internal/server"
)

// TestTopicStarWildcard matches a single word segment.
func TestTopicStarWildcard(t *testing.T) {
	srv := setupServer()
	ex := srv.ExchangeManager()
	qm := srv.QueueManager()
	bm := srv.BindingManager()
	store := srv.MessageStore()
	pub := srv.Publisher()

	_, _ = ex.Declare("ex.topic", server.ExchangeTopic,
		false, false, false, nil)
	_, _ = qm.Declare("q.usd", false, false, false, nil, nil)
	_, _ = qm.Declare("q.all", false, false, false, nil, nil)
	_, _ = bm.Bind("ex.topic", "q.usd", "stock.*.nyse", nil)
	_, _ = bm.Bind("ex.topic", "q.all", "stock.#", nil)

	msg := server.NewMessage([]byte("usd-nyse"), server.Properties{})
	msg.SetRoutingMeta("ex.topic", "stock.usd.nyse")
	_, _ = pub.Publish("ex.topic", "stock.usd.nyse", msg, 1)

	if store.Len("q.usd") != 1 {
		t.Fatalf("q.usd len = %d, want 1", store.Len("q.usd"))
	}
	if store.Len("q.all") != 1 {
		t.Fatalf("q.all len = %d, want 1", store.Len("q.all"))
	}
}

// TestTopicHashWildcard matches zero or more words.
func TestTopicHashWildcard(t *testing.T) {
	srv := setupServer()
	ex := srv.ExchangeManager()
	qm := srv.QueueManager()
	bm := srv.BindingManager()
	store := srv.MessageStore()
	pub := srv.Publisher()

	_, _ = ex.Declare("ex.topic", server.ExchangeTopic,
		false, false, false, nil)
	_, _ = qm.Declare("q.catchall", false, false, false, nil, nil)
	_, _ = bm.Bind("ex.topic", "q.catchall", "#", nil)

	cases := []string{
		"a",
		"a.b",
		"a.b.c.d.e",
	}
	for _, key := range cases {
		msg := server.NewMessage([]byte(key), server.Properties{})
		msg.SetRoutingMeta("ex.topic", key)
		_, _ = pub.Publish("ex.topic", key, msg, 1)
	}

	if store.Len("q.catchall") != 3 {
		t.Fatalf("len = %d, want 3", store.Len("q.catchall"))
	}
}

// TestTopicExactMatch routes only when routing key equals binding key
// and no wildcards are present.
func TestTopicExactMatch(t *testing.T) {
	srv := setupServer()
	ex := srv.ExchangeManager()
	qm := srv.QueueManager()
	bm := srv.BindingManager()
	store := srv.MessageStore()
	pub := srv.Publisher()

	_, _ = ex.Declare("ex.topic", server.ExchangeTopic,
		false, false, false, nil)
	_, _ = qm.Declare("q.exact", false, false, false, nil, nil)
	_, _ = bm.Bind("ex.topic", "q.exact", "news.tech", nil)

	msg := server.NewMessage([]byte("match"), server.Properties{})
	msg.SetRoutingMeta("ex.topic", "news.tech")
	_, _ = pub.Publish("ex.topic", "news.tech", msg, 1)

	if store.Len("q.exact") != 1 {
		t.Fatalf("len = %d, want 1", store.Len("q.exact"))
	}

	// Non-matching key should not land in the queue.
	msg2 := server.NewMessage([]byte("no-match"), server.Properties{})
	msg2.SetRoutingMeta("ex.topic", "news.sports")
	_, _ = pub.Publish("ex.topic", "news.sports", msg2, 1)

	if store.Len("q.exact") != 1 {
		t.Fatalf("len = %d, want 1 after non-match",
			store.Len("q.exact"))
	}
}

// TestTopicMixedWildcards verifies a combination of * and # in the
// same exchange with multiple bindings.
func TestTopicMixedWildcards(t *testing.T) {
	srv := setupServer()
	ex := srv.ExchangeManager()
	qm := srv.QueueManager()
	bm := srv.BindingManager()
	store := srv.MessageStore()
	pub := srv.Publisher()

	_, _ = ex.Declare("ex.topic", server.ExchangeTopic,
		false, false, false, nil)

	queues := []struct {
		qName string
		bind  string
	}{
		{"q1", "stock.usd.nyse"},
		{"q2", "stock.*.nyse"},
		{"q3", "stock.#"},
		{"q4", "#.nyse"},
		{"q5", "*.*.nyse"},
		{"q6", "#"},
	}
	for _, q := range queues {
		_, _ = qm.Declare(q.qName, false, false, false, nil, nil)
		_, _ = bm.Bind("ex.topic", q.qName, q.bind, nil)
	}

	msg := server.NewMessage([]byte("mixed"), server.Properties{})
	msg.SetRoutingMeta("ex.topic", "stock.usd.nyse")
	_, _ = pub.Publish("ex.topic", "stock.usd.nyse", msg, 1)

	expect := map[string]int{
		"q1": 1,
		"q2": 1,
		"q3": 1,
		"q4": 1,
		"q5": 1,
		"q6": 1,
	}
	for qName, want := range expect {
		if store.Len(qName) != want {
			t.Fatalf("%s len = %d, want %d", qName,
				store.Len(qName), want)
		}
	}
}

// TestTopicNoMatchWhenStarDemandsMore confirms that a single-*
// pattern does not match fewer segments.
func TestTopicNoMatchWhenStarDemandsMore(t *testing.T) {
	srv := setupServer()
	ex := srv.ExchangeManager()
	qm := srv.QueueManager()
	bm := srv.BindingManager()
	store := srv.MessageStore()
	pub := srv.Publisher()

	_, _ = ex.Declare("ex.topic", server.ExchangeTopic,
		false, false, false, nil)
	_, _ = qm.Declare("q1", false, false, false, nil, nil)
	_, _ = bm.Bind("ex.topic", "q1", "a.*.b", nil)

	// "a.b" has only two segments; pattern needs three.
	msg := server.NewMessage([]byte("short"), server.Properties{})
	msg.SetRoutingMeta("ex.topic", "a.b")
	_, _ = pub.Publish("ex.topic", "a.b", msg, 1)

	if store.Len("q1") != 0 {
		t.Fatalf("len = %d, want 0", store.Len("q1"))
	}

	// "a.x.b" has exactly three segments and should match.
	msg2 := server.NewMessage([]byte("exact"), server.Properties{})
	msg2.SetRoutingMeta("ex.topic", "a.x.b")
	_, _ = pub.Publish("ex.topic", "a.x.b", msg2, 1)

	if store.Len("q1") != 1 {
		t.Fatalf("len = %d, want 1", store.Len("q1"))
	}
}

// TestTopicPayloadIntegrity confirms the original payload survives
// topic routing.
func TestTopicPayloadIntegrity(t *testing.T) {
	srv := setupServer()
	ex := srv.ExchangeManager()
	qm := srv.QueueManager()
	bm := srv.BindingManager()
	store := srv.MessageStore()
	pub := srv.Publisher()

	_, _ = ex.Declare("ex.topic", server.ExchangeTopic,
		false, false, false, nil)
	_, _ = qm.Declare("q1", false, false, false, nil, nil)
	_, _ = bm.Bind("ex.topic", "q1", "#", nil)

	payload := []byte("payload-check-12345")
	msg := server.NewMessage(payload, server.Properties{})
	msg.SetRoutingMeta("ex.topic", "a.b.c")
	_, _ = pub.Publish("ex.topic", "a.b.c", msg, 1)

	m, _ := store.Dequeue("q1")
	if !bytes.Equal(m.Payload(), payload) {
		t.Fatalf("payload corrupted: %q", m.Payload())
	}
}
