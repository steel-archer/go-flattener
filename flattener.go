package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"

	"github.com/segmentio/kafka-go"
)

var server = "localhost:9092"
var inputTopic string
var destinationTopic string
var destinationSignature map[string]interface{}
var conversionMap map[string]string

func init() {
	inputTopic, destinationTopic, destinationSignature, conversionMap = readFlattenerConfig()
}

func readFlattenerConfig() (string, string, map[string]interface{}, map[string]string) {
	file, err := ioutil.ReadFile("flatteners.json")

	if err != nil {
		fmt.Println("Error while reading flatteners.json:", err)
		os.Exit(1)
	}

	configText := string(file)
	var fullConfig []map[string]interface{}
	json.Unmarshal([]byte(configText), &fullConfig)

	// Read topics' names
	inputTopic = fullConfig[0]["inputTopic"].(string)
	destinationTopic = fullConfig[0]["destinationTopic"].(string)

	// Read inputSignature and make conversionMap
	inputSignature := fullConfig[0]["graph"].(map[string]interface{})
	conversionMap = make(map[string]string)
	createConversionMap(inputSignature, conversionMap)

	// Read destinationSignature
	destinationSignature := fullConfig[0]["destinationMessage"].(map[string]interface{})

	return inputTopic, destinationTopic, destinationSignature, conversionMap
}

func createConversionMap(destinationSignature map[string]interface{}, conversionMap map[string]string) {
	for key, value := range destinationSignature {
		if reflect.TypeOf(value).Kind() == reflect.Map {
			createConversionMap(value.(map[string]interface{}), conversionMap)
		} else if reflect.ValueOf(value).Kind() == reflect.Slice {
			for _, val := range value.([]interface{}) {
				createConversionMap(val.(map[string]interface{}), conversionMap)
			}
		} else {
			conversionMap[value.(string)] = key
		}
	}
}

// Infinite loop of consumer -> read -> transform -> write
func main() {
	/*reader := getReader()
	writer := getWriter()
	defer reader.Close()
	defer writer.Close()*/
	for {
		/*fmt.Println("Reading input from topic", inputTopic)
		rawInput, err := reader.ReadMessage(context.Background())
		fmt.Println("Errors:", err)
		if err != nil {
			break
		}
		stringInput := string(rawInput.Value)
		fmt.Println("Input body:", stringInput)*/
		// Convert one input message to some amount of output messages
		stringInput := `{"Action":"something","Message":{"partitions":[{"name":"c:","driveType":3,"metric":{"usedSpaceBytes":342734824,"totalSpaceBytes":34273482423}},{"name":"d:","driveType":3,"metric":{"usedSpaceBytes":942734824,"totalSpaceBytes":904273482423}}],"createAtTimeUTC":"2017-08-07T08:38:43.3059476Z"}}`
		fmt.Println(stringInput)
		os.Exit(0)
		fmt.Println("Converting messages")
		outputMessages := convertMessage(stringInput)
		// And send them
		fmt.Println("Writing messages to topic", destinationTopic, ":", outputMessages)
		//err = writeTextMessage(writer, outputMessages)
		//fmt.Println("Errors:", err)
	}
}

func getReader() *kafka.Reader {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{server},
		Topic:     inputTopic,
		Partition: 0, // ATM read from the beginning
	})
	return reader
}

func getWriter() *kafka.Writer {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{server},
		Topic:    destinationTopic,
		Balancer: &kafka.LeastBytes{},
	})
	return writer
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

func writeTextMessage(writer *kafka.Writer, messages []interface{}) error {
	var err error
	for _, message := range messages {
		json, err := json.Marshal(message)
		if err != nil {
			break
		}
		fmt.Println("Jsoned output:", string(json))
		err = writer.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(""),
			Value: json,
		})
		if err != nil {
			break
		}
	}
	return err
}
