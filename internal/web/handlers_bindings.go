// handlers_bindings.go implements the Bindings API for the web UI.
package web

import "net/http"

// BindingsBroker is the subset of broker state needed for bindings.
type BindingsBroker interface {
	BindingList() []BindingInfo
}

var bindingsBroker BindingsBroker

// SetBindingsBroker injects the bindings data provider.
func SetBindingsBroker(b BindingsBroker) {
	bindingsBroker = b
}

func (s *Server) handleBindings(
	w http.ResponseWriter,
	r *http.Request,
) {
	if bindingsBroker == nil {
		writeJSON(w, []BindingInfo{})
		return
	}
	list := bindingsBroker.BindingList()
	if filter := r.URL.Query().Get("exchange"); filter != "" {
		filtered := make([]BindingInfo, 0, len(list))
		for _, b := range list {
			if b.Exchange == filter {
				filtered = append(filtered, b)
			}
		}
		list = filtered
	}
	writeJSON(w, list)
}
