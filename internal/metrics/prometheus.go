// prometheus.go implements a Prometheus-backed Collector.
package metrics

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// PrometheusCollector holds all Prometheus metrics for gomq.
type PrometheusCollector struct {
	connections    prometheus.Gauge
	messagesPub    prometheus.Counter
	messagesCon    prometheus.Counter
	messagesAck    prometheus.Counter
	messagesNack   prometheus.Counter
	queueCount     prometheus.Gauge
	consumerCount  prometheus.Gauge
	nodeUp         prometheus.Gauge
	registry       *prometheus.Registry
}

// NewPrometheusCollector creates a collector with registered metrics.
func NewPrometheusCollector() *PrometheusCollector {
	r := prometheus.NewRegistry()

	pc := &PrometheusCollector{
		registry: r,
		connections: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "gomq",
			Name:      "connections_total",
			Help:      "Current number of open connections.",
		}),
		messagesPub: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "gomq",
			Name:      "messages_published_total",
			Help:      "Total messages published.",
		}),
		messagesCon: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "gomq",
			Name:      "messages_consumed_total",
			Help:      "Total messages delivered to consumers.",
		}),
		messagesAck: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "gomq",
			Name:      "messages_acked_total",
			Help:      "Total messages acknowledged.",
		}),
		messagesNack: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "gomq",
			Name:      "messages_nacked_total",
			Help:      "Total messages negatively acknowledged.",
		}),
		queueCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "gomq",
			Name:      "queues_total",
			Help:      "Current number of declared queues.",
		}),
		consumerCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "gomq",
			Name:      "consumers_total",
			Help:      "Current number of active consumers.",
		}),
		nodeUp: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "gomq",
			Name:      "node_up",
			Help:      "Whether the node is up (1) or down (0).",
		}),
	}

	r.MustRegister(
		pc.connections,
		pc.messagesPub,
		pc.messagesCon,
		pc.messagesAck,
		pc.messagesNack,
		pc.queueCount,
		pc.consumerCount,
		pc.nodeUp,
	)

	return pc
}

// ConnectionOpened increments the connection gauge.
func (p *PrometheusCollector) ConnectionOpened() { p.connections.Inc() }

// ConnectionClosed decrements the connection gauge.
func (p *PrometheusCollector) ConnectionClosed() { p.connections.Dec() }

// MessagePublished increments the publish counter.
func (p *PrometheusCollector) MessagePublished() { p.messagesPub.Inc() }

// MessageConsumed increments the consume counter.
func (p *PrometheusCollector) MessageConsumed() { p.messagesCon.Inc() }

// MessageAcked increments the ack counter.
func (p *PrometheusCollector) MessageAcked() { p.messagesAck.Inc() }

// MessageNacked increments the nack counter.
func (p *PrometheusCollector) MessageNacked() { p.messagesNack.Inc() }

// QueueDeclared increments the queue gauge.
func (p *PrometheusCollector) QueueDeclared() { p.queueCount.Inc() }

// QueueDeleted decrements the queue gauge.
func (p *PrometheusCollector) QueueDeleted() { p.queueCount.Dec() }

// ConsumerAdded increments the consumer gauge.
func (p *PrometheusCollector) ConsumerAdded() { p.consumerCount.Inc() }

// ConsumerRemoved decrements the consumer gauge.
func (p *PrometheusCollector) ConsumerRemoved() { p.consumerCount.Dec() }

// NodeUp sets the node gauge to 1.
func (p *PrometheusCollector) NodeUp() { p.nodeUp.Set(1) }

// NodeDown sets the node gauge to 0.
func (p *PrometheusCollector) NodeDown() { p.nodeUp.Set(0) }

// Handler returns the HTTP handler for /metrics.
func (p *PrometheusCollector) Handler() http.Handler {
	return promhttp.HandlerFor(p.registry, promhttp.HandlerOpts{})
}

// ListenAndServe starts the metrics HTTP server on addr.
func (p *PrometheusCollector) ListenAndServe(addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", p.Handler())
	return http.ListenAndServe(addr, mux)
}

// RegisterWithMux mounts the /metrics handler on an existing mux.
func (p *PrometheusCollector) RegisterWithMux(
	mux *http.ServeMux, prefix string,
) {
	path := "/metrics"
	if prefix != "" {
		path = fmt.Sprintf("%s/metrics", prefix)
	}
	mux.Handle(path, p.Handler())
}
