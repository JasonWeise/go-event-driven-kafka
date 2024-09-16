package events

import "time"

// Order represents the domain model for an order
type Order struct {
	ID          string
	Description string
	Status      string
	CreatedAt   time.Time
}

// OrderCreatedEvent for creating orders
type OrderCreatedEvent struct {
	OrderID     string
	EventType   string
	Description string
}

type OrderUpdatedEvent struct {
	OrderID     string
	EventType   string
	Description string
}

type OrderDeletedEvent struct {
	OrderID   string
	EventType string
}
