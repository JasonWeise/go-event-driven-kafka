package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var kafkaTopic string = "bedaig-topic"

func main() {

	// Create a Kafka producer r publisher
	publisher, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer publisher.Close()

	// Delivery report handler
	go func() {
		for e := range publisher.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Successfully produced record to topic %s partition [%d] @ offset %v\n",
						*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			}
		}
	}()

	// list of messages to publish
	messageTexts := []string{
		`{"orderId": 11, "status": "success"}`, `{"orderId": 45, "status": "failed"}`,
		`{"orderId": 67, "status": "pending"}`, `{"orderId": 90, "status": "initial"}`,
	}

	// publish each message body sequentially
	for i := 0; i < len(messageTexts); i++ {
		messageValue := messageTexts[i]
		err := publisher.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
			Value:          []byte(messageValue),
		}, nil)

		if err != nil {
			log.Printf("Failed to publisher message: %v due to error: ", err)
		}
	}

	// Flush and wait for outstanding messages and requests to complete delivery.
	publisher.Flush(15 * 1000)
}
