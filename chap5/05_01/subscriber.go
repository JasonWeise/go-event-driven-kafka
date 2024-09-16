package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Subscriber defines the interface for a Kafka consumer
type Subscriber interface {
	Subscribe(topic string) error
}

// MessageHandler defines the function signature for handling messages
type MessageHandler func(msg *kafka.Message)

// KafkaSubscriber is the concrete implementation of the Subscriber interface using confluent-kafka-go
type KafkaSubscriber struct {
	consumer      *kafka.Consumer
	handleMessage MessageHandler
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

	return &KafkaSubscriber{consumer: consumer, handleMessage: handleMessage}, nil
}

// Subscribe subscribes to the specified topics and starts consuming messages
func (ks *KafkaSubscriber) Subscribe(eventTopic string) error {
	err := ks.consumer.SubscribeTopics([]string{eventTopic}, nil)
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
			msg, err := ks.consumer.ReadMessage(-1)
			if err != nil {
				// Handle error if needed
				fmt.Printf("Error consuming message: %v\n", err)
				continue
			}
			ks.handleMessage(msg)
		}
	}
}

func (ks *KafkaSubscriber) close() error {
	return ks.consumer.Close()
}
