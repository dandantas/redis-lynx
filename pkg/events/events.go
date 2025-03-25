package events

import "time"

// EventType represents the type of Redis event
type EventType string

const (
	// EventTypeSet represents a key set event
	EventTypeSet EventType = "set"

	// EventTypeExpired represents a key expired event
	EventTypeExpired EventType = "expired"

	// EventTypeDel represents a key deleted event
	EventTypeDel EventType = "del"
)

// Event represents a Redis keyspace event
type Event struct {
	// Type is the event type (set, expired, etc.)
	Type EventType `json:"type"`

	// Key is the key that triggered the event
	Key string `json:"key"`

	// Value is the value of the key (only for set events)
	Value string `json:"value,omitempty"`

	// Size is the size of the value in bytes (only for set events)
	Size int `json:"size,omitempty"`

	// TTL is the time-to-live of the key (only for set events with expiry)
	TTL *time.Duration `json:"ttl,omitempty"`

	// Database refers to the database witch the event was dispatched
	Database string `json:"database,omitempty"`

	// Timestamp is when the event occurred
	Timestamp time.Time `json:"timestamp"`
}

// NewEvent creates a new event with the current timestamp
func NewEvent(eventType EventType, key string, database string) *Event {
	return &Event{
		Type:      eventType,
		Key:       key,
		Database:  database,
		Timestamp: time.Now(),
	}
}

// WithValue adds a value to the event
func (e *Event) WithValue(value string) *Event {
	e.Value = value
	// Also calculate and set the size when value is added
	e.Size = len(value)
	return e
}

// WithTTL adds a TTL to the event
func (e *Event) WithTTL(ttl time.Duration) *Event {
	e.TTL = &ttl
	return e
}

// WithSize adds a size to the event
func (e *Event) WithSize(size int) *Event {
	e.Size = size
	return e
}
