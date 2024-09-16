package main

import (
	"fmt"

	"github.com/google/uuid"

	"eda/chap4/04_02/consumer"
	"eda/chap4/04_02/handlers"
)

func main() {
	// Kafka configuration
	broker := "localhost:9092"
	topic := "orders"
	groupID := "order-consumer-group"

	// Initialize Kafka producer
	producer, err := handlers.NewKafkaProducer(broker, topic)
	if err != nil {
		panic(fmt.Sprintf("Failed to create Kafka producer: %v", err))
	}

	// Initialize command handler
	commandHandler := handlers.NewCommandHandler(producer)

	orderId := uuid.New().String()

	// Example commands
	commandHandler.CreateOrder(orderId, "New Item")
	commandHandler.UpdateOrder(orderId, "Updated Item")
	commandHandler.DeleteOrder(orderId)

	// Initialize read model
	readModel := consumer.NewReadModel()

	// Initialize Kafka consumer
	consumer, err := consumer.NewKafkaConsumer(broker, groupID, topic, readModel)
	if err != nil {
		panic(fmt.Sprintf("Failed to create Kafka consumer: %v", err))
	}

	// Start the consumer to listen for events and update the read model
	consumer.Start()
}
