// handlers_i18n.go serves language packs and handles language switching.
package web

import (
	"net/http"
)

func (s *Server) handleI18n(w http.ResponseWriter, r *http.Request) {
	lang := s.i18n.DetectLang(r)
	pack := s.i18n.Pack(lang)
	if pack == nil {
		pack = make(map[string]string)
	}
	writeJSON(w, pack)
}

func (s *Server) handleSetLang(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	lang := r.FormValue("lang")
	if lang == "" {
		http.Error(w, "missing lang", http.StatusBadRequest)
		return
	}
	if !s.i18n.Has(lang) {
		http.Error(w, "unsupported language", http.StatusBadRequest)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     "lang",
		Value:    lang,
		Path:     "/",
		MaxAge:   86400 * 365,
		HttpOnly: false,
		SameSite: http.SameSiteLaxMode,
	})
	writeJSON(w, map[string]string{"lang": lang})
}
