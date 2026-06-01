// websocket_test.go tests the WebSocket hub and broadcaster.
package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestHubBroadcast(t *testing.T) {
	hub := NewHub()

	// Create a test server for WebSocket upgrade.
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			ServeWebSocket(hub, w, r)
		}))
	defer server.Close()

	// Connect a client.
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer ws.Close()

	// Give the server time to register the client.
	time.Sleep(100 * time.Millisecond)

	// Broadcast a message.
	broadcaster := NewBroadcaster(hub)
	broadcaster.BroadcastConnectionOpen("127.0.0.1:12345", "open")

	// Read the message.
	ws.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, msg, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var event Event
	if err := json.Unmarshal(msg, &event); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if event.Type != EventConnectionOpen {
		t.Fatalf("type = %s, want %s", event.Type, EventConnectionOpen)
	}
}

func TestBroadcasterNilHub(t *testing.T) {
	b := NewBroadcaster(nil)
	// Should not panic.
	b.BroadcastConnectionOpen("127.0.0.1:12345", "open")
	b.BroadcastQueueDepthChanged("q1", 5, 3)
}

func TestServerWithWebSocket(t *testing.T) {
	hub := NewHub()
	srv := NewServerWithWebSocket(hub)

	// Verify WebSocket route is registered.
	req := httptest.NewRequest("GET", "/ws", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	// Should attempt upgrade (returns 400 because not a valid WS request).
	if w.Code == http.StatusNotFound {
		t.Fatal("websocket route not registered")
	}
}
