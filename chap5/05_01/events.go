package main

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Event is the base structure for all events
type EventMetadata struct {
	ID        string
	Timestamp time.Time
	EventType string
}

type PaymentUpdatedEvent struct {
	Metadata EventMetadata
	Body     PaymentUpdatedEventPayload
}

type PaymentUpdatedEventPayload struct {
	PaymentID  string
	OrderId    string
	OccurredAt time.Time
	Amount     int
	Status     string
	Currency   string
}

// SerializeEvent serializes an event into []byte.
func (event PaymentUpdatedEvent) SerializeEvent() ([]byte, error) {
	return json.Marshal(event)
}

func DeserializeEvent(data []byte, event interface{}) error {
	return json.Unmarshal(data, event)
}

func NewPaymentUpdatedEvent() PaymentUpdatedEvent {
	return PaymentUpdatedEvent{
		Metadata: EventMetadata{
			ID:        uuid.New().String(),
			EventType: "PaymentUpdated",
			Timestamp: time.Now(),
		},
		Body: PaymentUpdatedEventPayload{
			PaymentID:  uuid.NewString(),
			OrderId:    uuid.NewString(),
			OccurredAt: time.Now().Add(3 * time.Hour),
			Amount:     12000,
			Status:     "success",
			Currency:   "usd",
		},
	}
}
