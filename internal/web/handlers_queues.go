// handlers_queues.go implements the Queues API for the web UI.
package web

import "net/http"

// QueuesBroker is the subset of broker state needed for queues.
type QueuesBroker interface {
	QueueList() []QueueInfo
}

var queuesBroker QueuesBroker

// SetQueuesBroker injects the queues data provider.
func SetQueuesBroker(b QueuesBroker) {
	queuesBroker = b
}

func (s *Server) handleQueues(
	w http.ResponseWriter,
	r *http.Request,
) {
	if queuesBroker == nil {
		writeJSON(w, []QueueInfo{})
		return
	}
	list := queuesBroker.QueueList()
	if filter := r.URL.Query().Get("durable"); filter != "" {
		want := filter == "true"
		filtered := make([]QueueInfo, 0, len(list))
		for _, q := range list {
			if q.Durable == want {
				filtered = append(filtered, q)
			}
		}
		list = filtered
	}
	writeJSON(w, list)
}
