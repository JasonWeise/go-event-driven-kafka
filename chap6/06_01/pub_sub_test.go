package pubsub_test

import (
	"errors"
	"testing"

	pubsub "eda/chap6/06_01"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

// Mocking Kafka Producer
type MockKafkaProducer struct {
	events      chan kafka.Event
	producedMsg *kafka.Message
	produceErr  error
	CloseCalled bool // Flag to indicate if Close was called
}

func (m *MockKafkaProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	m.producedMsg = msg
	return m.produceErr
}

func (m *MockKafkaProducer) Flush(timeoutMs int) int {
	return 0
}

func (m *MockKafkaProducer) Close() {
	// Set CloseCalled to true when this method is called
	m.CloseCalled = true
}

func NewMockKafkaProducer(produceErr error) *MockKafkaProducer {
	return &MockKafkaProducer{
		events:      make(chan kafka.Event),
		producedMsg: nil,
		produceErr:  produceErr,
	}
}

var (
	testTopic   = "test-topic"
	testMessage = []byte("test message")
)

// Table-driven test for KafkaProducer Send and Close methods
func TestKafkaProducer(t *testing.T) {
	tests := []struct {
		name           string
		topic          string
		message        []byte
		produceError   error
		expectedError  error
		expectedOutput []byte
		expectedTopic  string
	}{
		{
			name:           "Successfully Sending A Message",
			topic:          testTopic,
			message:        testMessage,
			produceError:   nil,
			expectedError:  nil,
			expectedOutput: []byte("test message"),
			expectedTopic:  "test-topic",
		},
		{
			name:           "Failing To Send A Message",
			topic:          testTopic,
			message:        testMessage,
			produceError:   errors.New("producer error"),
			expectedError:  errors.New("producer error"),
			expectedOutput: nil,
			expectedTopic:  "test-topic",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockProducer := NewMockKafkaProducer(tt.produceError)

			producer := &pubsub.KafkaProducer{
				Topic:         tt.topic,
				KafkaProducer: mockProducer,
			}

			err := producer.Send(tt.message)

			// Check if the error matches the expected error
			if tt.expectedError != nil {
				assert.EqualError(t, err, tt.expectedError.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedOutput, mockProducer.producedMsg.Value)
				assert.Equal(t, tt.expectedTopic, *mockProducer.producedMsg.TopicPartition.Topic)
			}
		})
	}
}

// Test KafkaProducer Close method
func TestKafkaProducer_Close(t *testing.T) {
	mockProducer := &MockKafkaProducer{}

	// Ensure Close() hasn't been called yet
	if mockProducer.CloseCalled {
		t.Error("Close() should not be called yet")
	}

	// Call Close()
	mockProducer.Close()

	// Verify Close() was called
	if !mockProducer.CloseCalled {
		t.Error("Expected Close() to be called, but it wasn't")
	}
}
