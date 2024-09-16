package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"eda/chap6"
)

const (
	testTopic      = "test-topic"
	testGroupID    = "test-group"
	kafkaBroker    = "localhost:9092"
	messageCount   = 5
	timeoutSeconds = 10
)

func TestKafkaIntegration(t *testing.T) {
	// Create a Kafka admin client to create the test topic
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": kafkaBroker})
	require.NoError(t, err)
	defer adminClient.Close()

	// Create the test topic
	err = createTopic(adminClient, testTopic)
	require.NoError(t, err)

	// Create a Kafka producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaBroker})
	require.NoError(t, err)
	defer producer.Close()

	// Create a KafkaEventPublisher
	publisher := pubsub.NewKafkaEventPublisher(producer)

	// Create a channel to receive consumed messages
	messagesChan := make(chan *kafka.Message, messageCount)

	// Create a KafkaSubscriber
	subscriber, err := pubsub.NewKafkaSubscriber(kafkaBroker, testGroupID, func(msg *kafka.Message) {
		messagesChan <- msg
	})
	require.NoError(t, err)

	// Start consuming messages in a separate goroutine
	go func() {
		err := subscriber.Subscribe(testTopic)
		if err != nil {
			t.Errorf("Error subscribing to topic: %v", err)
		}
	}()

	// Publish test messages
	ctx := context.Background()
	for i := 0; i < messageCount; i++ {
		event := pubsub.NewPaymentUpdatedEvent()
		eventBytes, err := event.SerializeEvent()
		require.NoError(t, err)

		err = publisher.Publish(ctx, testTopic, eventBytes)
		require.NoError(t, err)
		fmt.Printf("Published event: %d \n", i)
	}

	// Wait for all messages to be consumed or timeout
	receivedMessages := make([]*kafka.Message, 0, messageCount)
	timeout := time.After(timeoutSeconds * time.Second)

	for len(receivedMessages) < messageCount {
		select {
		case msg := <-messagesChan:
			receivedMessages = append(receivedMessages, msg)
		case <-timeout:
			t.Fatalf("Timed out waiting for messages. Received %d out of %d", len(receivedMessages), messageCount)
		}
	}

	// Verify the received messages
	for i, msg := range receivedMessages {
		var receivedEvent pubsub.PaymentUpdatedEvent
		err := pubsub.DeserializeEvent(msg.Value, &receivedEvent)
		require.NoError(t, err)

		assert.Equal(t, "PaymentUpdated", receivedEvent.Metadata.EventType)
		assert.NotEmpty(t, receivedEvent.Metadata.ID)
		assert.NotZero(t, receivedEvent.Metadata.Timestamp)
		assert.NotEmpty(t, receivedEvent.Body.PaymentID)
		assert.NotEmpty(t, receivedEvent.Body.OrderId)
		assert.Equal(t, 12000, receivedEvent.Body.Amount)
		assert.Equal(t, "success", receivedEvent.Body.Status)
		assert.Equal(t, "usd", receivedEvent.Body.Currency)

		fmt.Printf("Received message %d: %+v\n", i+1, receivedEvent)
	}

	// Clean up: close the subscriber and delete the test topic
	err = subscriber.Close()
	require.NoError(t, err)

	err = deleteTopic(adminClient, testTopic)
	require.NoError(t, err)
}

func createTopic(adminClient *kafka.AdminClient, topic string) error {
	_, err := adminClient.CreateTopics(context.Background(), []kafka.TopicSpecification{{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}})
	return err
}

func deleteTopic(adminClient *kafka.AdminClient, topic string) error {
	_, err := adminClient.DeleteTopics(context.Background(), []string{topic})
	return err
}
