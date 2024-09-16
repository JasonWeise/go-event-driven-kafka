package handlers

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"eda/chap4/04_02/events"
)

// KafkaProducer represents a Kafka producer
type KafkaProducer struct {
	producer *kafka.Producer
	topic    string
}

// NewKafkaProducer creates a new Kafka producer
func NewKafkaProducer(broker, topic string) (*KafkaProducer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		return nil, err
	}
	return &KafkaProducer{producer: p, topic: topic}, nil
}

// ProduceMessage produces a message to Kafka
func (kp *KafkaProducer) ProduceMessage(eventType string, event interface{}) error {
	message, err := json.Marshal(event)
	if err != nil {
		return err
	}

	err = kp.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &kp.topic, Partition: kafka.PartitionAny},
		Value:          message,
		Key:            []byte(eventType),
		Headers:        []kafka.Header{{Key: "eventType", Value: []byte(eventType)}},
	}, nil)
	if err != nil {
		return err
	}

	// Wait for message deliveries
	kp.producer.Flush(1000)
	return nil
}

// CommandHandler handles commands and publishes events to Kafka
type CommandHandler struct {
	producer *KafkaProducer
}

// NewCommandHandler creates a new CommandHandler
func NewCommandHandler(producer *KafkaProducer) *CommandHandler {
	return &CommandHandler{producer: producer}
}

// CreateOrder creates a new order and publishes an OrderCreatedEvent
func (ch *CommandHandler) CreateOrder(orderID, description string) {
	eventType := "OrderCreated"
	event := events.OrderCreatedEvent{OrderID: orderID, Description: description, EventType: eventType}
	err := ch.producer.ProduceMessage(eventType, event)
	if err != nil {
		log.Printf("Failed to produce message: %v", err)
	} else {
		fmt.Println("OrderCreatedEvent published")
	}
}

// UpdateOrder updates an existing order and publishes an OrderUpdatedEvent
func (ch *CommandHandler) UpdateOrder(orderID, description string) {
	eventType := "OrderUpdated"
	event := events.OrderUpdatedEvent{OrderID: orderID, Description: description, EventType: eventType}
	err := ch.producer.ProduceMessage(eventType, event)
	if err != nil {
		log.Printf("Failed to produce message: %v", err)
	} else {
		fmt.Println("OrderUpdatedEvent published")
	}
}

// DeleteOrder deletes an order and publishes an OrderDeletedEvent
func (ch *CommandHandler) DeleteOrder(orderID string) {
	eventType := "OrderDeleted"
	event := events.OrderDeletedEvent{OrderID: orderID, EventType: eventType}
	err := ch.producer.ProduceMessage(eventType, event)
	if err != nil {
		log.Printf("Failed to produce message: %v", err)
	} else {
		fmt.Println("OrderDeletedEvent published")
	}
}
