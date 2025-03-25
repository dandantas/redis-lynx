package websocket

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dandantas/redis-lynx/internal/config"
	"github.com/dandantas/redis-lynx/pkg/events"
)

func TestNewServer(t *testing.T) {
	// Create a config
	cfg := &config.Config{
		WebSocketPort: 8080,
	}

	// Create a test event channel
	eventChan := make(chan *events.Event)

	// Create a new server
	server := NewServer(cfg, eventChan)

	// Verify that the server was created successfully
	if server == nil {
		t.Fatal("Server is nil")
	}

	// Verify that the server has the correct configuration
	if server.config != cfg {
		t.Fatal("Server has incorrect configuration")
	}

	// Verify that the server has the correct event channel
	if server.events != eventChan {
		t.Fatal("Server has incorrect event channel")
	}

	// Verify that the server has an empty clients map
	if len(server.clients) != 0 {
		t.Fatal("Server has clients on creation")
	}

	// Verify that the register channel is not nil
	if server.register == nil {
		t.Fatal("Server register channel is nil")
	}

	// Verify that the unregister channel is not nil
	if server.unregister == nil {
		t.Fatal("Server unregister channel is nil")
	}
}

func TestMarshalEvent(t *testing.T) {
	// Create an event
	event := events.NewEvent(events.EventTypeSet, "test-key")
	event.WithValue("test-value")
	ttl := 60 * time.Second
	event.WithTTL(ttl)

	// Marshal event to JSON
	jsonEvent, err := marshalEvent(event)
	if err != nil {
		t.Fatalf("Error marshaling event: %v", err)
	}

	// Verify that the JSON contains the expected fields
	jsonStr := string(jsonEvent)
	if !strings.Contains(jsonStr, `"type":"set"`) {
		t.Errorf("JSON doesn't contain event type: %s", jsonStr)
	}

	if !strings.Contains(jsonStr, `"key":"test-key"`) {
		t.Errorf("JSON doesn't contain key: %s", jsonStr)
	}

	if !strings.Contains(jsonStr, `"value":"test-value"`) {
		t.Errorf("JSON doesn't contain value: %s", jsonStr)
	}

	if !strings.Contains(jsonStr, `"ttl":60000000000`) {
		t.Errorf("JSON doesn't contain TTL: %s", jsonStr)
	}

	if !strings.Contains(jsonStr, `"timestamp":`) {
		t.Errorf("JSON doesn't contain timestamp: %s", jsonStr)
	}
}

func TestWebSocketHandler(t *testing.T) {
	// Create a config
	cfg := &config.Config{
		WebSocketPort: 8080,
	}

	// Create a test event channel
	eventChan := make(chan *events.Event)

	// Create a new server
	server := NewServer(cfg, eventChan)

	// Create a test server
	s := httptest.NewServer(http.HandlerFunc(server.serveWs))
	defer s.Close()

	// Note: we don't actually connect here because it's complex to test
	// the actual WebSocket connection in a unit test. This just tests
	// that the handler is registered.
}

func TestCloseServer(t *testing.T) {
	// Create a config
	cfg := &config.Config{
		WebSocketPort: 8080,
	}

	// Create a test event channel
	eventChan := make(chan *events.Event)

	// Create a new server
	server := NewServer(cfg, eventChan)

	// Create a mock HTTP server
	server.httpServer = &http.Server{
		Addr: ":8080",
	}

	// Start the server in a goroutine
	go func() {
		// This will block until the server is shut down
		_ = server.httpServer.ListenAndServe()
	}()

	// Give the server time to start
	time.Sleep(100 * time.Millisecond)

	// Close the server
	if err := server.Close(); err != nil {
		t.Fatalf("Failed to close server: %v", err)
	}

	// Verify that the context is canceled
	select {
	case <-server.ctx.Done():
		// Success - context was canceled
	default:
		t.Fatal("Context was not canceled when server was closed")
	}
}
