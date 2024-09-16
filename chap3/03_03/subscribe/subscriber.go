package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var kafkaTopic string = "bedaig-topic"

func main() {
	// Create Kafka subscriber
	subscriber, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "my-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Failed to create subscriber: %s", err)
	}
	defer subscriber.Close()

	// Subscribe to topic
	err = subscriber.SubscribeTopics([]string{kafkaTopic}, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topic: %s", err)
	}

	fmt.Println("Subscriber started, waiting for messages...")

	// Poll for messages
	for {
		msg, err := subscriber.ReadMessage(-1)
		if err != nil {
			fmt.Printf("subscriber error: %v (%v)\n", err, msg)
			continue
		}
		fmt.Printf("Consumed message: %s: %s\n", msg.TopicPartition, string(msg.Value))
	}
}
