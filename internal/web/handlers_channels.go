// handlers_channels.go implements the Channels API for the web UI.
package web

import "net/http"

// ChannelInfo holds channel data for the UI.
type ChannelInfo struct {
	ID            uint16 `json:"id"`
	Connection    string `json:"connection"`
	State         string `json:"state"`
	Consumers     int    `json:"consumers"`
	Unacked       int    `json:"unacked"`
	PrefetchCount int    `json:"prefetch_count"`
	PrefetchLimit int    `json:"prefetch_limit"`
}

// ChannelsBroker is the subset of broker state needed for channels.
type ChannelsBroker interface {
	ChannelList() []ChannelInfo
}

var channelsBroker ChannelsBroker

// SetChannelsBroker injects the channels data provider.
func SetChannelsBroker(b ChannelsBroker) {
	channelsBroker = b
}

func (s *Server) handleChannels(
	w http.ResponseWriter,
	r *http.Request,
) {
	if channelsBroker == nil {
		writeJSON(w, []ChannelInfo{})
		return
	}
	list := channelsBroker.ChannelList()
	if filter := r.URL.Query().Get("connection"); filter != "" {
		filtered := make([]ChannelInfo, 0, len(list))
		for _, ch := range list {
			if ch.Connection == filter {
				filtered = append(filtered, ch)
			}
		}
		list = filtered
	}
	writeJSON(w, list)
}
