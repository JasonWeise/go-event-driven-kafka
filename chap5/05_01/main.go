package main

import (
	"context"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	eventTopic = "payment-update"
	groupID    = "eventGroup"
)

func main() {

	// create publisher
	ctx := context.Background()

	eventProducer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		fmt.Printf("Failed to create producer: %s \n", err)
	}
	defer eventProducer.Close()

	publisher := NewKafkaEventPublisher(eventProducer)

	// publish messages
	paymentSuccessfulEvent := NewPaymentUpdatedEvent()
	message, err := paymentSuccessfulEvent.SerializeEvent()
	if err != nil {
		fmt.Printf("Failed to serialise event due to error: %s\n", err)
		return
	}

	err = publisher.Publish(ctx, eventTopic, message)
	if err != nil {
		fmt.Printf("Failed to publish event due to error: %s\n", err)
		return
	}

	fmt.Printf("Successfully published: %+v \n", paymentSuccessfulEvent)

	// Define a message handler
	handleMessageFunc := func(msg *kafka.Message) {
		fmt.Println("handling message . . .")
		var event PaymentUpdatedEvent
		err := DeserializeEvent(msg.Value, &event)
		if err != nil {
			fmt.Printf("Could not deserialize event due to error: %s \n", err)
			return
		}
		fmt.Printf("Successfully consumed message: %+v \n", event)

	}

	// Create a new Kafka subscriber
	subscriber, err := NewKafkaSubscriber("localhost:9092", groupID, handleMessageFunc)
	if err != nil {
		fmt.Printf("Error creating subscriber: %v\n", err)
		os.Exit(1)
	}
	defer func(subscriber *KafkaSubscriber) {
		err := subscriber.close()
		if err != nil {
			fmt.Printf("Error closing subscriber: %v\n", err)
		}
	}(subscriber)

	// Subscribe to topic and start consuming messages
	if err := subscriber.Subscribe(eventTopic); err != nil {
		fmt.Printf("Error subscribing to topic: %v\n", err)
		os.Exit(1)
	}
}
