package main

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func main() {
	var server = "localhost:9092"
	var topic = "destinationTopic"
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{server},
		Topic:     topic,
		Partition: 0,
	})
	defer reader.Close()
	for {
		msg, err := reader.ReadMessage(context.Background())
		fmt.Println("Errors:", err)
		if err != nil {
			break
		}
		fmt.Println("Message:", string(msg.Value))
	}
}
