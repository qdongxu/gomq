// exchange_headers_test.go tests headers exchange routing.
package server

import (
	"testing"
)

func TestHeadersExchangeAllMatch(t *testing.T) {
	he := &HeadersExchange{}
	bindings := []*Binding{
		{
			QueueName: "q1",
			Args: map[string]interface{}{
				"x-match": "all",
				"format":  "json",
				"version": "v2",
			},
		},
	}

	cases := []struct {
		name     string
		headers  map[string]interface{}
		expected []string
	}{
		{
			name: "exact match",
			headers: map[string]interface{}{
				"format": "json", "version": "v2",
			},
			expected: []string{"q1"},
		},
		{
			name: "extra headers allowed",
			headers: map[string]interface{}{
				"format": "json", "version": "v2", "extra": "ok",
			},
			expected: []string{"q1"},
		},
		{
			name: "missing header",
			headers: map[string]interface{}{
				"format": "json",
			},
			expected: nil,
		},
		{
			name: "wrong value",
			headers: map[string]interface{}{
				"format": "json", "version": "v1",
			},
			expected: nil,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := he.Route("", c.headers, bindings)
			if !sameStrings(got, c.expected) {
				t.Fatalf("Route = %v, want %v", got, c.expected)
			}
		})
	}
}

func TestHeadersExchangeAnyMatch(t *testing.T) {
	he := &HeadersExchange{}
	bindings := []*Binding{
		{
			QueueName: "q1",
			Args: map[string]interface{}{
				"x-match": "any",
				"format":  "json",
				"version": "v2",
			},
		},
	}

	cases := []struct {
		name     string
		headers  map[string]interface{}
		expected []string
	}{
		{
			name: "both match",
			headers: map[string]interface{}{
				"format": "json", "version": "v2",
			},
			expected: []string{"q1"},
		},
		{
			name: "one matches",
			headers: map[string]interface{}{
				"format": "json",
			},
			expected: []string{"q1"},
		},
		{
			name: "none match",
			headers: map[string]interface{}{
				"format": "xml",
			},
			expected: nil,
		},
		{
			name: "empty headers",
			headers: map[string]interface{}{},
			expected: nil,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := he.Route("", c.headers, bindings)
			if !sameStrings(got, c.expected) {
				t.Fatalf("Route = %v, want %v", got, c.expected)
			}
		})
	}
}

func TestHeadersExchangeDefaultAll(t *testing.T) {
	he := &HeadersExchange{}
	bindings := []*Binding{
		{
			QueueName: "q1",
			Args: map[string]interface{}{
				"format": "json",
			},
		},
	}

	got := he.Route("", map[string]interface{}{"format": "json"}, bindings)
	if !sameStrings(got, []string{"q1"}) {
		t.Fatalf("default all match failed: %v", got)
	}

	got2 := he.Route("", map[string]interface{}{}, bindings)
	if len(got2) != 0 {
		t.Fatalf("default all no-match failed: %v", got2)
	}
}

func TestHeadersExchangeDeduplicate(t *testing.T) {
	he := &HeadersExchange{}
	bindings := []*Binding{
		{QueueName: "q1", Args: map[string]interface{}{"a": "1"}},
		{QueueName: "q1", Args: map[string]interface{}{"b": "2"}},
	}

	got := he.Route("", map[string]interface{}{"a": "1", "b": "2"}, bindings)
	if len(got) != 1 || got[0] != "q1" {
		t.Fatalf("dedup failed: %v", got)
	}
}

func TestHeadersExchangeNoBindings(t *testing.T) {
	he := &HeadersExchange{}
	got := he.Route("", map[string]interface{}{"a": "1"}, nil)
	if len(got) != 0 {
		t.Fatalf("nil bindings: %v", got)
	}
}

func TestHeadersExchangeIntMatch(t *testing.T) {
	he := &HeadersExchange{}
	bindings := []*Binding{
		{
			QueueName: "q1",
			Args: map[string]interface{}{
				"priority": int(5),
			},
		},
	}

	got := he.Route("", map[string]interface{}{"priority": int(5)}, bindings)
	if !sameStrings(got, []string{"q1"}) {
		t.Fatalf("int match failed: %v", got)
	}
}

func TestValuesEqual(t *testing.T) {
	cases := []struct {
		a, b   interface{}
		expect bool
	}{
		{"hello", "hello", true},
		{"hello", "world", false},
		{true, true, true},
		{true, false, false},
		{int(5), int(5), true},
		{int(5), int64(5), true},
		{int64(5), int(5), true},
		{uint(5), uint(5), true},
		{float32(1.5), float64(1.5), true},
		{float64(1.5), float32(1.5), true},
		{"hello", int(5), false},
		{int(5), "hello", false},
	}

	for _, c := range cases {
		got := valuesEqual(c.a, c.b)
		if got != c.expect {
			t.Fatalf("valuesEqual(%v, %v) = %v, want %v",
				c.a, c.b, got, c.expect)
		}
	}
}
