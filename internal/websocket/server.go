package websocket

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/dandantas/redis-lynx/internal/config"
	"github.com/dandantas/redis-lynx/pkg/events"
	"github.com/gorilla/websocket"
)

const (
	// Time allowed to write a message to the peer
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer
	maxMessageSize = 512
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// Allow connections from any origin
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Client represents a connected WebSocket client
type Client struct {
	// The WebSocket connection
	conn *websocket.Conn

	// Buffered channel of outbound messages
	send chan []byte

	// Server reference
	server *Server
}

// Server represents a WebSocket server that handles client connections
type Server struct {
	// Configuration
	config *config.Config

	// Connected clients
	clients map[*Client]bool

	// Inbound events from Redis
	events <-chan *events.Event

	// Register requests from clients
	register chan *Client

	// Unregister requests from clients
	unregister chan *Client

	// Mutex for protecting clients map
	mu sync.Mutex

	// HTTP server
	httpServer *http.Server

	// Context for graceful shutdown
	ctx context.Context

	// Cancel function for context
	cancel context.CancelFunc
}

// NewServer creates a new WebSocket server
func NewServer(cfg *config.Config, events <-chan *events.Event) *Server {
	ctx, cancel := context.WithCancel(context.Background())

	return &Server{
		config:     cfg,
		events:     events,
		clients:    make(map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Start starts the WebSocket server
func (s *Server) Start() error {
	// Set up HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.serveWs)

	// Create HTTP server
	s.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.config.WebSocketPort),
		Handler: mux,
	}

	// Start listening for client connections and events
	go s.run()

	// Start HTTP server
	log.Printf("WebSocket server listening on port %d", s.config.WebSocketPort)
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("error starting WebSocket server: %w", err)
	}

	return nil
}

// run processes client connections and events
func (s *Server) run() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case client := <-s.register:
			s.mu.Lock()
			s.clients[client] = true
			s.mu.Unlock()
			log.Printf("Client connected, total clients: %d", len(s.clients))
		case client := <-s.unregister:
			s.mu.Lock()
			if _, ok := s.clients[client]; ok {
				delete(s.clients, client)
				close(client.send)
			}
			s.mu.Unlock()
			log.Printf("Client disconnected, total clients: %d", len(s.clients))
		case event := <-s.events:
			// Marshal event to JSON
			fmt.Printf("Received event in wss: %+v\n", event)
			jsonEvent, err := marshalEvent(event)
			if err != nil {
				log.Printf("Error marshaling event: %v", err)
				continue
			}

			// Broadcast event to all clients
			s.broadcast(jsonEvent)
		}
	}
}

// broadcast sends a message to all connected clients
func (s *Server) broadcast(message []byte) {
	s.mu.Lock()
	for client := range s.clients {
		select {
		case client.send <- message:
		default:
			// Client can't keep up with messages, disconnect them
			close(client.send)
			delete(s.clients, client)
		}
	}
	s.mu.Unlock()
}

// Close closes the WebSocket server
func (s *Server) Close() error {
	// Signal to stop run loop
	s.cancel()

	// Close HTTP server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return s.httpServer.Shutdown(ctx)
}

// serveWs handles WebSocket requests from clients
func (s *Server) serveWs(w http.ResponseWriter, r *http.Request) {
	// Upgrade connection to WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Error upgrading connection: %v", err)
		return
	}

	// Create client
	client := &Client{
		conn:   conn,
		send:   make(chan []byte, 256),
		server: s,
	}

	// Register client
	s.register <- client

	// Start goroutines for reading and writing
	go client.writePump()
	go client.readPump()
}

// readPump pumps messages from the WebSocket connection to the hub
func (c *Client) readPump() {
	defer func() {
		c.server.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		// Read message
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("Error reading message: %v", err)
			}
			break
		}

		// We're only sending messages, not receiving them
		// But we need to keep the connection alive
	}
}

// writePump pumps messages from the hub to the WebSocket connection
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// The hub closed the channel
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			// Add queued messages to the current websocket message
			n := len(c.send)
			for i := 0; i < n; i++ {
				w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// Helper function to marshal events to JSON
func marshalEvent(event *events.Event) ([]byte, error) {
	return json.Marshal(event)
}
