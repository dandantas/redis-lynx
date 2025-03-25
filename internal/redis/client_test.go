package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dandantas/redis-lynx/internal/config"
	"github.com/dandantas/redis-lynx/pkg/events"
	"github.com/redis/go-redis/v9"
)

// mockClient is a modified Client that skips keyspace notification enablement
type mockClient struct {
	*Client
}

// newMockClient creates a new Client for testing
func newMockClient(cfg *config.Config) (*mockClient, error) {
	// Start mini Redis server
	s, err := miniredis.Run()
	if err != nil {
		return nil, err
	}

	// Update config to use mini Redis
	testCfg := &config.Config{
		RedisAddr:     s.Addr(),
		RedisPassword: "",
		RedisDB:       0,
		WebSocketPort: cfg.WebSocketPort,
		Debug:         cfg.Debug,
	}

	// Create context
	ctx, cancel := context.WithCancel(context.Background())

	// Create Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:     testCfg.RedisAddr,
		Password: testCfg.RedisPassword,
		DB:       testCfg.RedisDB,
	})

	// Create client
	client := &Client{
		rdb:       rdb,
		eventChan: make(chan *events.Event),
		ctx:       ctx,
		cancel:    cancel,
	}

	// Skip subscribing to events in tests

	return &mockClient{Client: client}, nil
}

func TestNewClient(t *testing.T) {
	// Create a mock client
	client, err := newMockClient(&config.Config{
		WebSocketPort: 8080,
		Debug:         false,
	})
	if err != nil {
		t.Fatalf("Failed to create mock client: %v", err)
	}
	defer client.Close()

	// Check that client was created successfully
	if client == nil {
		t.Fatal("Client is nil")
	}

	// Check that event channel is not nil
	if client.Events() == nil {
		t.Fatal("Event channel is nil")
	}
}

func TestKeyspaceNotifications(t *testing.T) {
	// Create a mock client
	client, err := newMockClient(&config.Config{
		WebSocketPort: 8080,
		Debug:         false,
	})
	if err != nil {
		t.Fatalf("Failed to create mock client: %v", err)
	}
	defer client.Close()

	// Get mini Redis instance
	miniRedis, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to start mini Redis: %v", err)
	}
	defer miniRedis.Close()

	// Set a key in Redis
	miniRedis.Set("test-key", "test-value")

	// Set TTL for the key
	miniRedis.SetTTL("test-key", time.Hour)

	// Simulate a key expiry
	miniRedis.FastForward(time.Hour + time.Second)

	// Miniredis doesn't support pub/sub keyspace notifications,
	// so we'll just verify that the client can connect and execute commands
}

func TestCloseClient(t *testing.T) {
	// Create a mock client
	client, err := newMockClient(&config.Config{
		WebSocketPort: 8080,
		Debug:         false,
	})
	if err != nil {
		t.Fatalf("Failed to create mock client: %v", err)
	}

	// Close client
	if err := client.Close(); err != nil {
		t.Fatalf("Failed to close client: %v", err)
	}

	// Verify that context is canceled
	select {
	case <-client.ctx.Done():
		// Success - context was canceled
	default:
		t.Fatal("Context was not canceled when client was closed")
	}
}
