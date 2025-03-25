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

	// TTL is the time-to-live of the key (only for set events with expiry)
	TTL *time.Duration `json:"ttl,omitempty"`

	// Timestamp is when the event occurred
	Timestamp time.Time `json:"timestamp"`
}

// NewEvent creates a new event with the current timestamp
func NewEvent(eventType EventType, key string) *Event {
	return &Event{
		Type:      eventType,
		Key:       key,
		Timestamp: time.Now(),
	}
}

// WithValue adds a value to the event
func (e *Event) WithValue(value string) *Event {
	e.Value = value
	return e
}

// WithTTL adds a TTL to the event
func (e *Event) WithTTL(ttl time.Duration) *Event {
	e.TTL = &ttl
	return e
}
