package web

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

// upgrader configures the WebSocket upgrade parameters.
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // allow all origins for management UI
	},
}

// Hub maintains the set of active WebSocket clients and broadcasts
// messages to them.
type Hub struct {
	clients map[*Client]struct{}
	mu      sync.RWMutex
}

// NewHub creates a new Hub instance.
func NewHub() *Hub {
	return &Hub{
		clients: make(map[*Client]struct{}),
	}
}

// Register adds a client to the hub.
func (h *Hub) Register(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[c] = struct{}{}
}

// Unregister removes a client from the hub.
func (h *Hub) Unregister(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, c)
}

// Broadcast sends a JSON message to all connected clients.
func (h *Hub) Broadcast(v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		log.Printf("websocket broadcast marshal: %v", err)
		return
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	for c := range h.clients {
		select {
		case c.send <- data:
		default:
			// Client buffer full; drop message.
		}
	}
}

// Client is a WebSocket client connection.
type Client struct {
	hub  *Hub
	conn *websocket.Conn
	send chan []byte
}

// newClient creates a new Client.
func newClient(hub *Hub, conn *websocket.Conn) *Client {
	return &Client{
		hub:  hub,
		conn: conn,
		send: make(chan []byte, 256),
	}
}

// readPump pumps messages from the WebSocket connection to the hub.
// It handles client disconnections.
func (c *Client) readPump() {
	defer func() {
		c.hub.Unregister(c)
		_ = c.conn.Close()
	}()

	c.conn.SetReadLimit(512)
	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(
				err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure,
			) {
				log.Printf("websocket unexpected close: %v", err)
			}
			break
		}
	}
}

// writePump pumps messages from the hub to the WebSocket connection.
func (c *Client) writePump() {
	defer func() {
		_ = c.conn.Close()
	}()

	for data := range c.send {
		if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
			log.Printf("websocket write: %v", err)
			return
		}
	}
}

// ServeWebSocket handles WebSocket requests from the management UI.
func ServeWebSocket(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("websocket upgrade: %v", err)
		return
	}

	client := newClient(hub, conn)
	hub.Register(client)

	go client.writePump()
	go client.readPump()
}
