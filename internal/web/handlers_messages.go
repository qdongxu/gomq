// handlers_messages.go implements the Messages API for the web UI.
package web

import (
	"net/http"
	"strconv"
)

// MessagesBroker is the subset of broker state needed for messages.
type MessagesBroker interface {
	MessageList(queueName string, limit, offset int) []MessageInfo
}

var messagesBroker MessagesBroker

// SetMessagesBroker injects the messages data provider.
func SetMessagesBroker(b MessagesBroker) {
	messagesBroker = b
}

func (s *Server) handleMessages(
	w http.ResponseWriter,
	r *http.Request,
) {
	if messagesBroker == nil {
		writeJSON(w, []MessageInfo{})
		return
	}

	queue := r.URL.Query().Get("queue")
	if queue == "" {
		writeJSON(w, []MessageInfo{})
		return
	}

	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > 200 {
		limit = 200 // cap to avoid huge responses
	}

	offset := 0
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}

	list := messagesBroker.MessageList(queue, limit, offset)
	writeJSON(w, list)
}
