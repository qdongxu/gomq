// auth_test.go tests session, auth, and CSRF middleware.
package web

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLoginAndSession(t *testing.T) {
	srv := NewServer(AuthConfig{Username: "admin", Password: "secret"})

	// Unauthenticated request redirects to login.
	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusFound {
		t.Fatalf("unauth status = %d, want 302", w.Code)
	}
	loc := w.Result().Header.Get("Location")
	if !strings.Contains(loc, "/login") {
		t.Fatalf("unexpected redirect: %s", loc)
	}

	// Login with wrong password.
	req = httptest.NewRequest("POST", "/api/login", nil)
	req.Form = map[string][]string{
		"username": {"admin"},
		"password": {"wrong"},
	}
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("bad auth status = %d, want 401", w.Code)
	}

	// Login with correct credentials.
	req = httptest.NewRequest("POST", "/api/login", nil)
	req.Form = map[string][]string{
		"username": {"admin"},
		"password": {"secret"},
	}
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusFound {
		t.Fatalf("login status = %d, want 302", w.Code)
	}
	cookies := w.Result().Cookies()
	var sessionCookie *http.Cookie
	for _, c := range cookies {
		if c.Name == sessionCookieName {
			sessionCookie = c
			break
		}
	}
	if sessionCookie == nil {
		t.Fatal("missing session cookie after login")
	}
	if !sessionCookie.HttpOnly {
		t.Fatal("session cookie must be HttpOnly")
	}
	if sessionCookie.SameSite != http.SameSiteStrictMode {
		t.Fatalf("session cookie SameSite = %v, want Strict", sessionCookie.SameSite)
	}

	// Authenticated request succeeds.
	req = httptest.NewRequest("GET", "/", nil)
	req.AddCookie(sessionCookie)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("authed status = %d, want 200", w.Code)
	}

	// Logout clears cookie (POST requires CSRF token).
	token := srv.csrf.CSRFTokenForRequest(
		&http.Request{Header: http.Header{"Cookie": {sessionCookie.String()}}})
	req = httptest.NewRequest("POST", "/api/logout", nil)
	req.AddCookie(sessionCookie)
	req.Header.Set(csrfHeader, token)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusFound {
		t.Fatalf("logout status = %d, want 302", w.Code)
	}
	clearCookie := w.Result().Cookies()
	for _, c := range clearCookie {
		if c.Name == sessionCookieName && c.MaxAge < 0 {
			return
		}
	}
	t.Fatal("logout did not clear session cookie")
}

func TestCSRFProtection(t *testing.T) {
	srv := NewServer(AuthConfig{Username: "admin", Password: "admin"})
	cookie := loginCookie(srv)

	// Safe method (GET) without CSRF token is allowed.
	req := httptest.NewRequest("GET", "/api/overview", nil)
	req.AddCookie(cookie)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET without CSRF status = %d, want 200", w.Code)
	}

	// Unsafe method without CSRF token is rejected.
	req = httptest.NewRequest("POST", "/api/logout", nil)
	req.AddCookie(cookie)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("POST without CSRF status = %d, want 403", w.Code)
	}

	// Unsafe method with correct CSRF token succeeds.
	token := srv.csrf.CSRFTokenForRequest(
		&http.Request{Header: http.Header{"Cookie": {cookie.String()}}})
	req = httptest.NewRequest("POST", "/api/logout", nil)
	req.AddCookie(cookie)
	req.Header.Set(csrfHeader, token)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusFound {
		t.Fatalf("POST with CSRF status = %d, want 302", w.Code)
	}
}

func TestLoginPageAccessible(t *testing.T) {
	srv := NewServer(AuthConfig{Username: "admin", Password: "admin"})
	req := httptest.NewRequest("GET", "/login", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("login page status = %d, want 200", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "gomq login") {
		t.Fatal("missing login page title")
	}
	if !strings.Contains(body, "password") {
		t.Fatal("missing password field")
	}
}

func TestIndexCSRFMeta(t *testing.T) {
	srv := NewServer(AuthConfig{Username: "admin", Password: "admin"})
	req := httptest.NewRequest("GET", "/", nil)
	req.AddCookie(loginCookie(srv))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, `meta name="csrf-token"`) {
		t.Fatalf("missing CSRF meta tag in index, body:\n%s", body)
	}
	if !strings.Contains(body, "htmx:configRequest") {
		t.Fatal("missing htmx CSRF header injection script")
	}
}

func TestSessionExpirationCleanup(t *testing.T) {
	store := NewSessionStore()
	defer store.Stop()

	id, data := store.Create("testuser")
	if id == "" {
		t.Fatal("expected session id")
	}
	if data.Username != "testuser" {
		t.Fatalf("unexpected username: %s", data.Username)
	}
	if data.CSRFToken == "" {
		t.Fatal("expected csrf token")
	}

	// Session is retrievable.
	got, ok := store.Get(id)
	if !ok || got.Username != "testuser" {
		t.Fatal("session not found after creation")
	}

	// Delete session.
	store.Delete(id)
	_, ok = store.Get(id)
	if ok {
		t.Fatal("expected session to be deleted")
	}
}
