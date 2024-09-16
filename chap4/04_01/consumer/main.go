package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"eda/chap4/04_01/handlers"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	kafkaTopic := "order-events"

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "order-service",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
		return
	}

	err = consumer.SubscribeTopics([]string{"order-events"}, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topic: %s", err)
		return
	}

	fmt.Printf("started consumer. Listening on '%s' topic for messages......\n", kafkaTopic)

	run := true
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				switch string(e.Headers[0].Value) {
				case "OrderCreated":
					handlers.OrderCreatedHandler(e)
				case "OrderApproved":
					handlers.OrderApprovedHandler(e)
				case "OrderCancelled":
					handlers.OrderCancelledHandler(e)
				}
			case kafka.Error:
				fmt.Printf("Error: %v\n", e)
				run = false
			default:
				fmt.Printf("Ignored: %v\n", e)
			}
		}
	}

	fmt.Println("Closing consumer")

	err = consumer.Close()
	if err != nil {
		log.Fatalf("Failed to close consumer: %s", err)
	}
}
