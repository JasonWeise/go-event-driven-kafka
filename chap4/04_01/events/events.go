package events

import (
	"encoding/json"
	"time"
)

// BaseEvent represents the base structure for all events.
type BaseEvent struct {
	EventID   string    `json:"event_id"`
	EventType string    `json:"event_type"`
	Timestamp time.Time `json:"timestamp"`
}

// OrderCreated event
type OrderCreated struct {
	BaseEvent
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	Amount     float64 `json:"amount"`
}

// OrderApproved event
type OrderApproved struct {
	BaseEvent
	OrderID string `json:"order_id"`
}

// OrderCancelled event
type OrderCancelled struct {
	BaseEvent
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

// SerializeEvent serializes an event into []byte.
func SerializeEvent(event interface{}) ([]byte, error) {
	return json.Marshal(event)
}

func DeserializeEvent(data []byte, event interface{}) error {
	return json.Unmarshal(data, event)
}
