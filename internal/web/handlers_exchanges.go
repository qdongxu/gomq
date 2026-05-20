// handlers_exchanges.go implements the Exchanges API for the web UI.
package web

import "net/http"

// ExchangesBroker is the subset of broker state needed for exchanges.
type ExchangesBroker interface {
	ExchangeList() []ExchangeInfo
}

var exchangesBroker ExchangesBroker

// SetExchangesBroker injects the exchanges data provider.
func SetExchangesBroker(b ExchangesBroker) {
	exchangesBroker = b
}

func (s *Server) handleExchanges(
	w http.ResponseWriter,
	r *http.Request,
) {
	if exchangesBroker == nil {
		writeJSON(w, []ExchangeInfo{})
		return
	}
	list := exchangesBroker.ExchangeList()
	if filter := r.URL.Query().Get("type"); filter != "" {
		filtered := make([]ExchangeInfo, 0, len(list))
		for _, ex := range list {
			if ex.Type == filter {
				filtered = append(filtered, ex)
			}
		}
		list = filtered
	}
	writeJSON(w, list)
}
