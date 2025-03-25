package redis

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dandantas/redis-lynx/internal/config"
	"github.com/dandantas/redis-lynx/pkg/events"
	"github.com/redis/go-redis/v9"
)

const (
	// All keyspace events
	keyspaceNotifyValue = "KEA"

	// Redis keyspace notification channels
	keyeventPrefix    = "__keyevent@"
	keySetChannel     = "__keyevent@*__:set"
	keyExpiredChannel = "__keyevent@*__:expired"
	keyDeletedChannel = "__keyevent@*__:del"
)

// Client represents a Redis client with keyspace event subscription
type Client struct {
	// Redis client
	rdb *redis.Client

	// Channel for publishing events
	eventChan chan *events.Event

	// Context for graceful shutdown
	ctx context.Context

	// Function to cancel the context
	cancel context.CancelFunc
}

// NewClient creates a new Redis client
func NewClient(cfg *config.Config) (*Client, error) {
	// Create context with cancel
	ctx, cancel := context.WithCancel(context.Background())

	// Create Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	// Test connection
	if err := rdb.Ping(ctx).Err(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}
	fmt.Println("Connected to Redis")

	// Create client
	client := &Client{
		rdb:       rdb,
		eventChan: make(chan *events.Event),
		ctx:       ctx,
		cancel:    cancel,
	}

	// should connect to the redis database who we will be getting the data from
	// otherwise the Get command to another database will fail

	// We should receive only the events that the user want to listen
	// Enable keyspace notifications
	// if err := client.enableKeyspaceNotifications(); err != nil {
	// 	cancel()
	// 	return nil, fmt.Errorf("failed to enable keyspace notifications: %w", err)
	// }

	// Subscribe to keyspace events
	go client.subscribeToEvents()

	return client, nil
}

// enableKeyspaceNotifications enables keyspace notifications in Redis
func (c *Client) enableKeyspaceNotifications() error {
	cmd := c.rdb.ConfigSet(c.ctx, "notify-keyspace-events", keyspaceNotifyValue)
	return cmd.Err()
}

// subscribeToEvents subscribes to Redis keyspace events
func (c *Client) subscribeToEvents() {
	// Subscribe to keyspace events
	pubsub := c.rdb.PSubscribe(c.ctx, keySetChannel, keyExpiredChannel, keyDeletedChannel)
	defer pubsub.Close()

	// Listen for messages
	ch := pubsub.Channel()

	for {
		select {
		case <-c.ctx.Done():
			return
		case msg := <-ch:
			c.handleMessage(msg)
		}
	}
}

// handleMessage handles a Redis keyspace event
func (c *Client) handleMessage(msg *redis.Message) {
	// Parse channel to get event type
	parts := strings.Split(msg.Channel, ":")
	if len(parts) != 2 {
		return
	}
	// event: Message<__keyevent@5__:set: bluelabsocial/comgas/comgas:sunshine_67e31db567dd51032f951ccf:TTL2>
	eventPaths := strings.Split(parts[0], "@")
	if len(eventPaths) != 2 {
		return
	}

	// Parse database number from "__keyevent@{db}__"
	dbPart := strings.Split(eventPaths[1], "__")
	if len(dbPart) < 1 {
		return
	}

	// Store database number for the event
	database := dbPart[0]

	eventTypeStr := parts[1]
	key := msg.Payload

	var eventType events.EventType
	switch eventTypeStr {
	case "set":
		eventType = events.EventTypeSet
	case "expired":
		eventType = events.EventTypeExpired
	case "del":
		eventType = events.EventTypeDel
	default:
		return
	}

	// Create event
	event := events.NewEvent(eventType, key, database)

	// For set events, try to get the value and TTL
	if eventType == events.EventTypeSet {
		// Get value
		val, err := c.rdb.Get(c.ctx, key).Result()
		if err == nil {
			event.WithValue(val)
		}

		// Get TTL
		ttl, err := c.rdb.TTL(c.ctx, key).Result()
		if err == nil && ttl > 0 {
			event.WithTTL(ttl)
		}
	}

	// Send event to channel
	select {
	case c.eventChan <- event:
	case <-c.ctx.Done():
		return
	case <-time.After(time.Second):
		// Timeout if nobody is listening
		return
	}
}

// Events returns the event channel
func (c *Client) Events() <-chan *events.Event {
	return c.eventChan
}

// Close closes the Redis client
func (c *Client) Close() error {
	c.cancel()
	close(c.eventChan)
	return c.rdb.Close()
}
