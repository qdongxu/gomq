// metrics_test.go tests the Prometheus metrics collector.
package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPrometheusCollector_Registration(t *testing.T) {
	pc := NewPrometheusCollector()
	if pc == nil {
		t.Fatal("NewPrometheusCollector returned nil")
	}
}

func TestPrometheusCollector_Connections(t *testing.T) {
	pc := NewPrometheusCollector()
	pc.ConnectionOpened()
	pc.ConnectionOpened()
	pc.ConnectionClosed()

	body := fetchMetrics(t, pc)
	assertMetric(t, body, "gomq_connections_total", "1")
}

func TestPrometheusCollector_Messages(t *testing.T) {
	pc := NewPrometheusCollector()
	pc.MessagePublished()
	pc.MessagePublished()
	pc.MessageConsumed()
	pc.MessageAcked()
	pc.MessageNacked()

	body := fetchMetrics(t, pc)
	assertMetric(t, body, "gomq_messages_published_total", "2")
	assertMetric(t, body, "gomq_messages_consumed_total", "1")
	assertMetric(t, body, "gomq_messages_acked_total", "1")
	assertMetric(t, body, "gomq_messages_nacked_total", "1")
}

func TestPrometheusCollector_Queues(t *testing.T) {
	pc := NewPrometheusCollector()
	pc.QueueDeclared()
	pc.QueueDeclared()
	pc.QueueDeleted()

	body := fetchMetrics(t, pc)
	assertMetric(t, body, "gomq_queues_total", "1")
}

func TestPrometheusCollector_Consumers(t *testing.T) {
	pc := NewPrometheusCollector()
	pc.ConsumerAdded()
	pc.ConsumerAdded()
	pc.ConsumerRemoved()

	body := fetchMetrics(t, pc)
	assertMetric(t, body, "gomq_consumers_total", "1")
}

func TestPrometheusCollector_NodeUp(t *testing.T) {
	pc := NewPrometheusCollector()
	pc.NodeUp()

	body := fetchMetrics(t, pc)
	assertMetric(t, body, "gomq_node_up", "1")

	pc.NodeDown()
	body = fetchMetrics(t, pc)
	assertMetric(t, body, "gomq_node_up", "0")
}

func TestPrometheusCollector_HTTPHandler(t *testing.T) {
	pc := NewPrometheusCollector()
	pc.NodeUp()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	pc.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "gomq_node_up") {
		t.Fatal("expected gomq_node_up in output")
	}
}

func TestNoOp(t *testing.T) {
	// Ensure NoOp does not panic when called.
	n := &NoOp{}
	n.ConnectionOpened()
	n.ConnectionClosed()
	n.MessagePublished()
	n.MessageConsumed()
	n.MessageAcked()
	n.MessageNacked()
	n.QueueDeclared()
	n.QueueDeleted()
	n.ConsumerAdded()
	n.ConsumerRemoved()
	n.NodeUp()
	n.NodeDown()
}

// fetchMetrics renders the current metrics and returns the body.
func fetchMetrics(t *testing.T, pc *PrometheusCollector) string {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	pc.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics handler: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	return string(body)
}

// assertMetric checks that body contains name with the expected value.
func assertMetric(t *testing.T, body, name, value string) {
	t.Helper()
	needle := name + " " + value
	if !strings.Contains(body, needle) {
		t.Fatalf("expected %q in metrics output", needle)
	}
}
