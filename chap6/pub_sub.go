package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// EventMetadata  is the base structure for all events
type EventMetadata struct {
	ID        string
	Timestamp time.Time
	EventType string
}

type PaymentUpdatedEvent struct {
	Metadata EventMetadata
	Body     PaymentUpdatedEventPayload
}

type PaymentUpdatedEventPayload struct {
	PaymentID  string
	OrderId    string
	OccurredAt time.Time
	Amount     int
	Status     string
	Currency   string
}

// SerializeEvent serializes an event into []byte.
func (event PaymentUpdatedEvent) SerializeEvent() ([]byte, error) {
	return json.Marshal(event)
}

func DeserializeEvent(data []byte, event interface{}) error {
	return json.Unmarshal(data, event)
}

func NewPaymentUpdatedEvent() PaymentUpdatedEvent {
	return PaymentUpdatedEvent{
		Metadata: EventMetadata{
			ID:        uuid.New().String(),
			EventType: "PaymentUpdated",
			Timestamp: time.Now(),
		},
		Body: PaymentUpdatedEventPayload{
			PaymentID:  uuid.NewString(),
			OrderId:    uuid.NewString(),
			OccurredAt: time.Now().Add(3 * time.Hour),
			Amount:     12000,
			Status:     "success",
			Currency:   "usd",
		},
	}
}

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

// Subscriber defines the interface for a Kafka consumer
type Subscriber interface {
	Subscribe(topic string) error
}

// MessageHandler defines the function signature for handling messages
type MessageHandler func(msg *kafka.Message)

// KafkaSubscriber is the concrete implementation of the Subscriber interface using confluent-kafka-go
type KafkaSubscriber struct {
	Consumer      *kafka.Consumer
	HandleMessage MessageHandler
}

// NewKafkaSubscriber creates a new KafkaSubscriber
func NewKafkaSubscriber(brokers string, groupID string, handleMessage MessageHandler) (*KafkaSubscriber, error) {
	config := &kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		return nil, err
	}

	return &KafkaSubscriber{Consumer: consumer, HandleMessage: handleMessage}, nil
}

// Subscribe subscribes to the specified topics and starts consuming messages
func (ks *KafkaSubscriber) Subscribe(eventTopic string) error {
	err := ks.Consumer.SubscribeTopics([]string{eventTopic}, nil)
	if err != nil {
		fmt.Printf("Could not subscribe to topic. Error occurred: %s", err)
		return err
	}

	// Setup signal handling to gracefully shut down on interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Context to manage cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-sigChan
		fmt.Println("Received shutdown signal")
		cancel()
	}()

	fmt.Println("Started consuming messages...")
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Shutting down consumer...")
			return nil
		default:
			msg, err := ks.Consumer.ReadMessage(-1)
			if err != nil {
				// Handle error if needed
				fmt.Printf("Error consuming message: %v\n", err)
				continue
			}
			ks.HandleMessage(msg)
		}
	}
}

func (ks *KafkaSubscriber) Close() error {
	return ks.Consumer.Close()
}
