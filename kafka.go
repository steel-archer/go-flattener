package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
)

var inputTopic = "inputTopic"
var destinationTopic = "destinationTopic"
var server = "localhost:9092"

func init() {
}

func main() {
	//for true {
	inputMessage, _ := readTextMessage(inputTopic)
	fmt.Println(inputMessage)
	//break
	/*outputMessages := convertMessage(inputMessage)
	for _, outputMessage := range outputMessages {
		_ = writeTextMessage(destinationTopic, outputMessage)
	}*/
	//}
}

func readTextMessage(topic string) (messages []string, err error) {
	// make a new reader that consumes from topic-A, partition 0, at offset 42
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{server},
		Topic:     inputTopic,
		Partition: 0,
	})
	result := []string{}

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		result = append(result, string(m.Value))
		fmt.Println(result)
	}

	r.Close()

	return result, err
}

func convertMessage(inputMessage string) (outputMessages []interface{}) {
	var rawResult map[string]interface{}
	json.Unmarshal([]byte(inputMessage), &rawResult)

	messages := rawResult["Message"].(map[string]interface{})
	createAtTimeUTC := messages["createAtTimeUTC"]
	partitions := messages["partitions"].([]interface{})

	result := make([]interface{}, len(partitions)) // Make a result array

	for num, message := range partitions {
		currentMessage := message.(map[string]interface{})
		metric := currentMessage["metric"].(map[string]interface{})
		payloadData := map[string]interface{}{
			"name":            currentMessage["name"],
			"driveType":       currentMessage["driveType"],
			"usedSpaceBytes":  int(metric["usedSpaceBytes"].(float64)),  // Convert from float64 to int
			"totalSpaceBytes": int(metric["totalSpaceBytes"].(float64)), // Convert from float64 to int
			"createAtTimeUTC": createAtTimeUTC,
		}
		resultMessage := map[string]map[string]interface{}{
			"payloadData": payloadData,
		}
		result[num] = resultMessage
	}

	return result
}

func writeTextMessage(topic string, message interface{}) error {
	fmt.Println(topic)
	fmt.Println(message)
	var err error
	return err
}
