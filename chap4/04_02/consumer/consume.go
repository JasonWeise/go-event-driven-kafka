package consumer

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"eda/chap4/04_02/events"
)

// ReadModel represents a simple in-memory read model
type ReadModel struct {
	orders map[string]events.Order
	mu     sync.RWMutex
}

// NewReadModel creates a new ReadModel
func NewReadModel() *ReadModel {
	return &ReadModel{
		orders: make(map[string]events.Order),
	}
}

// ApplyOrderCreatedEvent applies an OrderCreatedEvent to the read model
func (rm *ReadModel) ApplyOrderCreatedEvent(event events.OrderCreatedEvent) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.orders[event.OrderID] = events.Order{
		ID:          event.OrderID,
		Description: event.Description,
		Status:      "Created",
		CreatedAt:   time.Now(),
	}
	fmt.Printf("Order %s created\n", event.OrderID)
}

// ApplyOrderUpdatedEvent applies an OrderUpdatedEvent to the read model
func (rm *ReadModel) ApplyOrderUpdatedEvent(event events.OrderUpdatedEvent) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if order, exists := rm.orders[event.OrderID]; exists {
		order.Description = event.Description
		rm.orders[event.OrderID] = order
		fmt.Printf("Order %s updated\n", event.OrderID)
	}
}

// ApplyOrderDeletedEvent applies an OrderDeletedEvent to the read model
func (rm *ReadModel) ApplyOrderDeletedEvent(event events.OrderDeletedEvent) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	delete(rm.orders, event.OrderID)
	fmt.Printf("Order %s deleted\n", event.OrderID)
}

// KafkaConsumer represents a Kafka consumer
type KafkaConsumer struct {
	consumer  *kafka.Consumer
	readModel *ReadModel
	topic     string
}

// NewKafkaConsumer creates a new Kafka consumer
func NewKafkaConsumer(broker, groupID, topic string, readModel *ReadModel) (*KafkaConsumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return nil, err
	}
	return &KafkaConsumer{consumer: c, readModel: readModel, topic: topic}, nil
}

// Start begins consuming messages from Kafka
func (kc *KafkaConsumer) Start() {
	err := kc.consumer.SubscribeTopics([]string{kc.topic}, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topics: %v", err)
	}
	fmt.Println("Kafka consumer started, waiting for messages...")

	run := true
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			msg, err := kc.consumer.ReadMessage(-1)
			if err == nil {
				kc.processMessage(msg)
			} else {
				fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			}
		}
	}

	fmt.Println("Closing consumer")

	err = kc.consumer.Close()
	if err != nil {
		log.Fatalf("Failed to close consumer: %s", err)
	}
}

func (kc *KafkaConsumer) processMessage(msg *kafka.Message) {
	var orderCreatedEvent events.OrderCreatedEvent
	var orderUpdatedEvent events.OrderUpdatedEvent
	var orderDeletedEvent events.OrderDeletedEvent

	switch string(msg.Headers[0].Value) {
	case "OrderCreated":
		if err := json.Unmarshal(msg.Value, &orderCreatedEvent); err == nil && orderCreatedEvent.OrderID != "" {
			kc.readModel.ApplyOrderCreatedEvent(orderCreatedEvent)
			return
		}
	case "OrderUpdated":
		if err := json.Unmarshal(msg.Value, &orderUpdatedEvent); err == nil && orderUpdatedEvent.OrderID != "" {
			kc.readModel.ApplyOrderUpdatedEvent(orderUpdatedEvent)
			return
		}

	case "OrderDeleted":
		if err := json.Unmarshal(msg.Value, &orderDeletedEvent); err == nil && orderDeletedEvent.OrderID != "" {
			kc.readModel.ApplyOrderDeletedEvent(orderDeletedEvent)
			return
		}
	default:
		fmt.Printf("Unknown message format: %s\n", string(msg.Value))
		return
	}
}
