package pubsub

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Producer interface that abstracts Kafka Producer operations
type Producer interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Flush(timeoutMs int) int
	Close()
}

// KafkaProducer struct
type KafkaProducer struct {
	BootstrapServers string
	Topic            string
	KafkaProducer    Producer
}

// NewKafkaProducer constructor
func NewKafkaProducer(bootstrapServers string, topic string) *KafkaProducer {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})

	if err != nil {
		log.Fatalf("Error creating Kafka producer: %v", err)
	}

	return &KafkaProducer{
		BootstrapServers: bootstrapServers,
		Topic:            topic,
		KafkaProducer:    producer,
	}
}

// Send message to Kafka broker
func (p *KafkaProducer) Send(value []byte) error {
	err := p.KafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.Topic, Partition: kafka.PartitionAny},
		Value:          value,
	}, nil)

	if err != nil {
		return err
	}
	p.KafkaProducer.Flush(15 * 1000)

	return nil
}

// Close the Kafka producer
func (p *KafkaProducer) Close() {
	p.KafkaProducer.Close()
}
