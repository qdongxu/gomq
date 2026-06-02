// csrf.go provides CSRF token generation and validation for htmx requests.
package web

import (
	"crypto/subtle"
	"fmt"
	"net/http"
)

const csrfHeader = "X-CSRF-Token"

// CSRFMiddleware validates CSRF tokens on state-changing requests.
type CSRFMiddleware struct {
	store *SessionStore
}

// NewCSRFMiddleware creates a CSRF middleware.
func NewCSRFMiddleware(store *SessionStore) *CSRFMiddleware {
	return &CSRFMiddleware{store: store}
}

// Protect wraps handlers with CSRF validation.
func (c *CSRFMiddleware) Protect(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/login" || r.URL.Path == "/api/login" {
			next(w, r)
			return
		}
		if isSafeMethod(r.Method) {
			next(w, r)
			return
		}
		id := ReadSessionID(r)
		if id == "" {
			http.Error(w, "session required", http.StatusForbidden)
			return
		}
		data, ok := c.store.Get(id)
		if !ok {
			http.Error(w, "session expired", http.StatusForbidden)
			return
		}
		token := r.Header.Get(csrfHeader)
		if token == "" {
			token = r.FormValue("csrf_token")
		}
		if token == "" || subtle.ConstantTimeCompare([]byte(token), []byte(data.CSRFToken)) != 1 {
			http.Error(w, "invalid csrf token", http.StatusForbidden)
			return
		}
		next(w, r)
	}
}

// CSRFTokenForRequest returns the CSRF token bound to the current session.
func (c *CSRFMiddleware) CSRFTokenForRequest(r *http.Request) string {
	id := ReadSessionID(r)
	if id == "" {
		return ""
	}
	data, ok := c.store.Get(id)
	if !ok {
		return ""
	}
	return data.CSRFToken
}

func isSafeMethod(m string) bool {
	return m == http.MethodGet || m == http.MethodHead || m == http.MethodOptions || m == http.MethodTrace
}

// CSRFMetaTag returns an HTML meta tag with the CSRF token for htmx.
func CSRFMetaTag(token string) string {
	return fmt.Sprintf(`<meta name="csrf-token" content="%s">`, token)
}
