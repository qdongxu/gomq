// session.go provides in-memory session storage with expiration
// cleanup for the management UI.
package web

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"sync"
	"time"
)

const (
	sessionCookieName = "gomq_session"
	sessionTTL        = 24 * time.Hour
	cleanupInterval   = 5 * time.Minute
)

// SessionData holds per-session state.
type SessionData struct {
	Username  string
	CSRFToken string
	CreatedAt time.Time
	AccessedAt time.Time
}

// SessionStore is an in-memory session manager.
type SessionStore struct {
	mu       sync.RWMutex
	sessions map[string]*SessionData
	quit     chan struct{}
}

// NewSessionStore creates a session store and starts the cleanup goroutine.
func NewSessionStore() *SessionStore {
	s := &SessionStore{
		sessions: make(map[string]*SessionData),
		quit:     make(chan struct{}),
	}
	go s.cleanupLoop()
	return s
}

// Stop shuts down the cleanup goroutine.
func (s *SessionStore) Stop() {
	close(s.quit)
}

// Create returns a new session ID and stores the data.
func (s *SessionStore) Create(username string) (string, *SessionData) {
	id := generateSessionID()
	now := time.Now()
	data := &SessionData{
		Username:   username,
		CSRFToken:  generateSessionID(),
		CreatedAt:  now,
		AccessedAt: now,
	}
	s.mu.Lock()
	s.sessions[id] = data
	s.mu.Unlock()
	return id, data
}

// Get retrieves a session by ID and refreshes its accessed time.
func (s *SessionStore) Get(id string) (*SessionData, bool) {
	s.mu.RLock()
	data, ok := s.sessions[id]
	s.mu.RUnlock()
	if !ok {
		return nil, false
	}
	if time.Since(data.AccessedAt) > sessionTTL {
		return nil, false
	}
	s.mu.Lock()
	data.AccessedAt = time.Now()
	s.mu.Unlock()
	return data, true
}

// Delete removes a session.
func (s *SessionStore) Delete(id string) {
	s.mu.Lock()
	delete(s.sessions, id)
	s.mu.Unlock()
}

// cleanupLoop periodically evicts expired sessions.
func (s *SessionStore) cleanupLoop() {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.evictExpired()
		case <-s.quit:
			return
		}
	}
}

func (s *SessionStore) evictExpired() {
	cutoff := time.Now().Add(-sessionTTL)
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, d := range s.sessions {
		if d.AccessedAt.Before(cutoff) {
			delete(s.sessions, id)
		}
	}
}

func generateSessionID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// SessionCookie returns a configured session cookie.
func SessionCookie(id string) *http.Cookie {
	return &http.Cookie{
		Name:     sessionCookieName,
		Value:    id,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   int(sessionTTL.Seconds()),
	}
}

// ClearSessionCookie returns a cookie that deletes the session.
func ClearSessionCookie() *http.Cookie {
	return &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   -1,
	}
}

// ReadSessionID extracts the session ID from the request.
func ReadSessionID(r *http.Request) string {
	c, err := r.Cookie(sessionCookieName)
	if err != nil {
		return ""
	}
	return c.Value
}
