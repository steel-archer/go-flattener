package main

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func main() {
	var server = "localhost:9092"
	var topic = "inputTopic"
	var msg = `{"Action":"something","Message":{"partitions":[{"name":"a:","driveType":1,"metric":{"usedSpaceBytes":222,"totalSpaceBytes":111}},{"name":"c:","driveType":3,"metric":{"usedSpaceBytes":342734824,"totalSpaceBytes":34273482423}},{"name":"d:","driveType":3,"metric":{"usedSpaceBytes":942734824,"totalSpaceBytes":904273482423}}],"createAtTimeUTC":"2017-08-07T08:38:43.3059476Z"}}`

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{server},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	err := writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(""),
		Value: []byte(msg),
	})
	fmt.Println("Errors:", err)
}
