// exchange_topic_test.go tests topic exchange pattern matching.
package server

import (
	"testing"
)

func TestTopicExchangeRoute(t *testing.T) {
	te := &TopicExchange{}
	bindings := []*Binding{
		{QueueName: "q1", RoutingKey: "stock.usd.nyse"},
		{QueueName: "q2", RoutingKey: "stock.*.nyse"},
		{QueueName: "q3", RoutingKey: "stock.#"},
		{QueueName: "q4", RoutingKey: "#.nyse"},
		{QueueName: "q5", RoutingKey: "*.*.nyse"},
		{QueueName: "q6", RoutingKey: "#"},
	}

	cases := []struct {
		key      string
		expected []string
	}{
		{"stock.usd.nyse", []string{"q1", "q2", "q3", "q4", "q5", "q6"}},
		{"stock.eur.nyse", []string{"q2", "q3", "q4", "q5", "q6"}},
		{"stock.usd.london", []string{"q3", "q6"}},
		{"nyse", []string{"q4", "q6"}},
		{"commodity.gold.nyse", []string{"q4", "q5", "q6"}},
	}

	for _, c := range cases {
		got := te.Route(c.key, bindings)
		if !sameStrings(got, c.expected) {
			t.Fatalf("Route(%q) = %v, want %v", c.key, got, c.expected)
		}
	}
}

func TestTopicExchangeNoMatch(t *testing.T) {
	te := &TopicExchange{}
	bindings := []*Binding{
		{QueueName: "q1", RoutingKey: "stock.*.nyse"},
	}

	cases := []string{
		"stock",
		"stock.usd",
		"stock.usd.nyse.close",
	}

	for _, key := range cases {
		got := te.Route(key, bindings)
		if len(got) != 0 {
			t.Fatalf("Route(%q) = %v, want []", key, got)
		}
	}
}

func TestTopicExchangeExact(t *testing.T) {
	te := &TopicExchange{}
	bindings := []*Binding{
		{QueueName: "q1", RoutingKey: "exact.key"},
	}

	got := te.Route("exact.key", bindings)
	if !sameStrings(got, []string{"q1"}) {
		t.Fatalf("exact match failed: %v", got)
	}

	got2 := te.Route("exact.other", bindings)
	if len(got2) != 0 {
		t.Fatalf("non-match returned: %v", got2)
	}
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[string]int)
	for _, s := range a {
		m[s]++
	}
	for _, s := range b {
		m[s]--
		if m[s] < 0 {
			return false
		}
	}
	return true
}
