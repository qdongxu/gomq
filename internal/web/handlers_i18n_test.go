// handlers_i18n_test.go tests the i18n handler and language switching.
package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandleI18n(t *testing.T) {
	srv := NewServer(AuthConfig{Username: "admin", Password: "admin"})
	req := httptest.NewRequest("GET", "/api/i18n", nil)
	w := httptest.NewRecorder()
	req.AddCookie(loginCookie(srv))
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var pack map[string]string
	if err := json.NewDecoder(w.Body).Decode(&pack); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if pack["title"] != "gomq management" {
		t.Fatalf("unexpected title: %q", pack["title"])
	}
	if pack["overview"] != "Overview" {
		t.Fatalf("unexpected overview: %q", pack["overview"])
	}
}

func TestHandleI18nChineseHeader(t *testing.T) {
	srv := NewServer(AuthConfig{Username: "admin", Password: "admin"})
	req := httptest.NewRequest("GET", "/api/i18n", nil)
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
	w := httptest.NewRecorder()
	req.AddCookie(loginCookie(srv))
	srv.ServeHTTP(w, req)

	var pack map[string]string
	if err := json.NewDecoder(w.Body).Decode(&pack); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if pack["title"] != "gomq 管理端" {
		t.Fatalf("unexpected title: %q", pack["title"])
	}
	if pack["overview"] != "概览" {
		t.Fatalf("unexpected overview: %q", pack["overview"])
	}
}

func TestHandleI18nCookieOverride(t *testing.T) {
	srv := NewServer(AuthConfig{Username: "admin", Password: "admin"})
	req := httptest.NewRequest("GET", "/api/i18n", nil)
	req.Header.Set("Accept-Language", "en")
	req.AddCookie(&http.Cookie{Name: "lang", Value: "ja"})
	w := httptest.NewRecorder()
	req.AddCookie(loginCookie(srv))
	srv.ServeHTTP(w, req)

	var pack map[string]string
	if err := json.NewDecoder(w.Body).Decode(&pack); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if pack["title"] != "gomq 管理画面" {
		t.Fatalf("unexpected title: %q", pack["title"])
	}
}

func TestHandleSetLang(t *testing.T) {
	srv := NewServer(AuthConfig{Username: "admin", Password: "admin"})
	cookie := loginCookie(srv)
	token := srv.csrf.CSRFTokenForRequest(
		&http.Request{Header: http.Header{"Cookie": {cookie.String()}}})
	req := httptest.NewRequest("POST", "/api/lang", strings.NewReader("lang=zh"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set(csrfHeader, token)
	w := httptest.NewRecorder()
	req.AddCookie(cookie)
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var out map[string]string
	if err := json.NewDecoder(w.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out["lang"] != "zh" {
		t.Fatalf("unexpected lang: %q", out["lang"])
	}

	// Verify cookie was set.
	cookies := w.Result().Cookies()
	var langCookie *http.Cookie
	for _, c := range cookies {
		if c.Name == "lang" {
			langCookie = c
			break
		}
	}
	if langCookie == nil || langCookie.Value != "zh" {
		t.Fatal("lang cookie not set correctly")
	}
}

func TestHandleSetLangInvalid(t *testing.T) {
	srv := NewServer(AuthConfig{Username: "admin", Password: "admin"})
	cookie := loginCookie(srv)
	token := srv.csrf.CSRFTokenForRequest(
		&http.Request{Header: http.Header{"Cookie": {cookie.String()}}})
	req := httptest.NewRequest("POST", "/api/lang", strings.NewReader("lang=fr"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set(csrfHeader, token)
	w := httptest.NewRecorder()
	req.AddCookie(cookie)
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestHandleSetLangMissing(t *testing.T) {
	srv := NewServer(AuthConfig{Username: "admin", Password: "admin"})
	cookie := loginCookie(srv)
	token := srv.csrf.CSRFTokenForRequest(
		&http.Request{Header: http.Header{"Cookie": {cookie.String()}}})
	req := httptest.NewRequest("POST", "/api/lang", strings.NewReader(""))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set(csrfHeader, token)
	w := httptest.NewRecorder()
	req.AddCookie(cookie)
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestHandleSetLangMethodNotAllowed(t *testing.T) {
	srv := NewServer(AuthConfig{Username: "admin", Password: "admin"})
	req := httptest.NewRequest("GET", "/api/lang", nil)
	w := httptest.NewRecorder()
	req.AddCookie(loginCookie(srv))
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", w.Code)
	}
}

func TestHandleIndexI18n(t *testing.T) {
	srv := NewServer(AuthConfig{Username: "admin", Password: "admin"})
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Accept-Language", "zh")
	w := httptest.NewRecorder()
	req.AddCookie(loginCookie(srv))
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "gomq 管理端") {
		t.Fatalf("missing Chinese title in body: %s", body)
	}
	if !strings.Contains(body, "概览") {
		t.Fatal("missing Chinese overview in body")
	}
	if !strings.Contains(body, "交换机") {
		t.Fatal("missing Chinese exchange term in body")
	}
	if !strings.Contains(body, "加载中") {
		t.Fatal("missing Chinese loading text in body")
	}
}

func TestHandleIndexLangSwitcher(t *testing.T) {
	srv := NewServer(AuthConfig{Username: "admin", Password: "admin"})
	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	req.AddCookie(loginCookie(srv))
	srv.ServeHTTP(w, req)

	body := w.Body.String()
	if !strings.Contains(body, `<select onchange="setLang(this.value)">`) {
		t.Fatal("missing language switcher in body")
	}
	if !strings.Contains(body, `value="en"`) {
		t.Fatal("missing English option in language switcher")
	}
	if !strings.Contains(body, `value="zh"`) {
		t.Fatal("missing Chinese option in language switcher")
	}
	if !strings.Contains(body, `value="ja"`) {
		t.Fatal("missing Japanese option in language switcher")
	}
}
