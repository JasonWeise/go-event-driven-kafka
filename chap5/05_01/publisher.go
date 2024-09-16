package main

import (
	"context"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Publisher defines the interface for publishing events.
type Publisher interface {
	Publish(ctx context.Context, eventTopic string, event []byte) error
}

// KafkaEventPublisher is the struct definition of a kafka based publisher interface implementation.
type KafkaEventPublisher struct {
	KafkaProducer *kafka.Producer
}

// NewKafkaEventPublisher creates a new instance of the KafkaPublisher struct.
func NewKafkaEventPublisher(producer *kafka.Producer) Publisher {
	return &KafkaEventPublisher{
		KafkaProducer: producer,
	}
}

// Publish enqueues an event using the Kafka Publisher.
func (p KafkaEventPublisher) Publish(ctx context.Context, eventTopic string, event []byte) error {
	err := p.KafkaProducer.Produce(
		&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &eventTopic,
				Partition: kafka.PartitionAny,
			},
			Value: event,
		}, nil)

	if err != nil {
		log.Printf("Failed to publisher message: %v due to error: ", err)
	}

	return err
}
