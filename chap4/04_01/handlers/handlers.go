package handlers

import (
	"fmt"

	"eda/chap4/04_01/events"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// OrderCreatedHandler handles the OrderCreated event.
func OrderCreatedHandler(msg *kafka.Message) {
	var event events.OrderCreated
	err := events.DeserializeEvent(msg.Value, &event)
	if err != nil {
		fmt.Printf("Error deserializing OrderCreated event: %v \n", err)
		return
	}

	fmt.Printf("Processing OrderCreated Event: OrderID: %s, CustomerID: %s, Amount: %f \n", event.OrderID, event.CustomerID, event.Amount)
	// Business logic to handle the OrderCreated event.
}

// OrderApprovedHandler handles the OrderApproved event.
func OrderApprovedHandler(msg *kafka.Message) {
	var event events.OrderApproved
	err := events.DeserializeEvent(msg.Value, &event)
	if err != nil {
		fmt.Printf("Error deserializing OrderApproved event: %v \n", err)
		return
	}

	fmt.Printf("Processing OrderApproved Event: OrderID: %s \n", event.OrderID)
	// Business logic to handle the OrderApproved event.
}

// OrderCancelledHandler handles the OrderCancelled event.
func OrderCancelledHandler(msg *kafka.Message) {
	var event events.OrderCancelled
	err := events.DeserializeEvent(msg.Value, &event)
	if err != nil {
		fmt.Printf("Error deserializing OrderCancelled event: %v \n", err)
		return
	}

	fmt.Printf("Processing OrderCancelled Event: OrderID: %s, Reason: %s \n", event.OrderID, event.Reason)
	// Business logic to handle the OrderCancelled event.
}
