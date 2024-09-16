package eventstore

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaEventStore struct {
	Producer *kafka.Producer
	Topic    string
}

// NewKafkaEventStore initializes a new KafkaEventStore.
func NewKafkaEventStore(broker string, topic string) (*KafkaEventStore, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		return nil, err
	}

	return &KafkaEventStore{
		Producer: p,
		Topic:    topic,
	}, nil
}

// PublishEvent publishes an event to Kafka.
func (store *KafkaEventStore) PublishEvent(eventType string, key string, value []byte) error {
	deliveryChan := make(chan kafka.Event)

	err := store.Producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &store.Topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          value,
		Headers:        []kafka.Header{{Key: "eventType", Value: []byte(eventType)}},
	}, deliveryChan)

	if err != nil {
		return err
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		return m.TopicPartition.Error
	}

	fmt.Printf("Delivered message to %v\n", m.TopicPartition)
	close(deliveryChan)
	return nil
}

// Close closes the producer connection.
func (store *KafkaEventStore) Close() {
	store.Producer.Close()
}
