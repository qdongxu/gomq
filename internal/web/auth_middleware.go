// auth_middleware.go provides session-based authentication for the
// management UI.
package web

import (
	"crypto/subtle"
	"net/http"
)

// AuthConfig holds the credentials for the management UI.
type AuthConfig struct {
	Username string
	Password string // bcrypt would be better; kept simple for MVP
}

// AuthMiddleware wraps handlers with session validation.
type AuthMiddleware struct {
	store  *SessionStore
	config AuthConfig
}

// NewAuthMiddleware creates an auth middleware.
func NewAuthMiddleware(store *SessionStore, config AuthConfig) *AuthMiddleware {
	return &AuthMiddleware{store: store, config: config}
}

// Require wraps a handler to require a valid session.
func (a *AuthMiddleware) Require(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/login" || r.URL.Path == "/api/login" {
			next(w, r)
			return
		}
		id := ReadSessionID(r)
		if id == "" {
			a.redirectLogin(w, r)
			return
		}
		_, ok := a.store.Get(id)
		if !ok {
			a.redirectLogin(w, r)
			return
		}
		next(w, r)
	}
}

func (a *AuthMiddleware) redirectLogin(w http.ResponseWriter, r *http.Request) {
	if isHTMX(r) {
		w.Header().Set("HX-Redirect", "/login")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	http.Redirect(w, r, "/login", http.StatusFound)
}

// HandleLogin validates credentials and creates a session.
func (a *AuthMiddleware) HandleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	username := r.FormValue("username")
	password := r.FormValue("password")
	if !constantTimeEq(username, a.config.Username) ||
		!constantTimeEq(password, a.config.Password) {
		http.Error(w, "invalid credentials", http.StatusUnauthorized)
		return
	}
	id, _ := a.store.Create(username)
	http.SetCookie(w, SessionCookie(id))
	if isHTMX(r) {
		w.Header().Set("HX-Redirect", "/")
		w.WriteHeader(http.StatusOK)
		return
	}
	http.Redirect(w, r, "/", http.StatusFound)
}

// HandleLogout destroys the session.
func (a *AuthMiddleware) HandleLogout(w http.ResponseWriter, r *http.Request) {
	id := ReadSessionID(r)
	if id != "" {
		a.store.Delete(id)
	}
	http.SetCookie(w, ClearSessionCookie())
	if isHTMX(r) {
		w.Header().Set("HX-Redirect", "/login")
		w.WriteHeader(http.StatusOK)
		return
	}
	http.Redirect(w, r, "/login", http.StatusFound)
}

func isHTMX(r *http.Request) bool {
	return r.Header.Get("HX-Request") == "true"
}

func constantTimeEq(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
