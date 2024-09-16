package main

import (
	"fmt"
	"log"
	"time"

	"eda/chap4/04_01/events"
	"eda/chap4/04_01/eventstore"

	"github.com/google/uuid"
)

func main() {
	// Initialize Kafka producer
	kafkaBroker := "localhost:9092" // Kafka broker address
	topic := "order-events"         // Topic to publish events to

	kafkaEventStore, err := eventstore.NewKafkaEventStore(kafkaBroker, topic)
	if err != nil {
		log.Fatalf("Failed to create Kafka event store: %s", err)
	}
	defer kafkaEventStore.Close()

	// Create a sample OrderCreated event
	orderID := uuid.New().String()
	orderCreatedEvent := events.OrderCreated{
		BaseEvent: events.BaseEvent{
			EventID:   uuid.New().String(),
			EventType: "OrderCreated",
			Timestamp: time.Now(),
		},
		OrderID:    orderID,
		CustomerID: "cust-1234",
		Amount:     150.00,
	}

	// Serialize the event
	serializedEvent, err := events.SerializeEvent(orderCreatedEvent)
	if err != nil {
		log.Fatalf("Failed to serialize event: %s", err)
	}

	// Publish the OrderCreated event
	err = kafkaEventStore.PublishEvent("OrderCreated", orderID, serializedEvent)
	if err != nil {
		log.Fatalf("Failed to publish OrderCreated event: %s", err)
	}
	fmt.Printf("Published OrderCreated event for OrderID: %s\n", orderID)

	// Simulate publishing OrderApproved and OrderCancelled events
	time.Sleep(2 * time.Second) // Wait for 2 seconds

	orderApprovedEvent := events.OrderApproved{
		BaseEvent: events.BaseEvent{
			EventID:   uuid.New().String(),
			EventType: "OrderApproved",
			Timestamp: time.Now(),
		},
		OrderID: orderID,
	}

	serializedApprovedEvent, err := events.SerializeEvent(orderApprovedEvent)
	if err != nil {
		log.Fatalf("Failed to serialize OrderApproved event: %s", err)
	}

	err = kafkaEventStore.PublishEvent("OrderApproved", orderID, serializedApprovedEvent)
	if err != nil {
		log.Fatalf("Failed to publish OrderApproved event: %s", err)
	}
	fmt.Printf("Published OrderApproved event for OrderID: %s\n", orderID)

	time.Sleep(2 * time.Second) // Wait for 2 seconds

	orderCancelledEvent := events.OrderCancelled{
		BaseEvent: events.BaseEvent{
			EventID:   uuid.New().String(),
			EventType: "OrderCancelled",
			Timestamp: time.Now(),
		},
		OrderID: orderID,
		Reason:  "Customer requested cancellation.",
	}

	serializedCancelledEvent, err := events.SerializeEvent(orderCancelledEvent)
	if err != nil {
		log.Fatalf("Failed to serialize OrderCancelled event: %s", err)
	}

	err = kafkaEventStore.PublishEvent("OrderCancelled", orderID, serializedCancelledEvent)
	if err != nil {
		log.Fatalf("Failed to publish OrderCancelled event: %s", err)
	}
	fmt.Printf("Published OrderCancelled event for OrderID: %s\n", orderID)
}
