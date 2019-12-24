package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strconv"

	"github.com/segmentio/kafka-go"
)

var server = "localhost:9092"
var inputTopic string
var destTopic string
var destSignature map[string]interface{}
var conversionMap map[string]string

func init() {
	inputTopic, destTopic, destSignature, conversionMap = readFlattenerConfig()
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
	destTopic = fullConfig[0]["destinationTopic"].(string)

	// Read inputSignature and make conversionMap
	inputSignature := fullConfig[0]["graph"].(map[string]interface{})["Message"].(map[string]interface{})
	conversionMap = make(map[string]string)
	createConversionMap(inputSignature, conversionMap)

	// Read destSignature
	destSignature := fullConfig[0]["destinationMessage"].(map[string]interface{})

	return inputTopic, destTopic, destSignature, conversionMap
}

func createConversionMap(destSignature map[string]interface{}, conversionMap map[string]string) {
	for key, value := range destSignature {
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
	reader := getReader(server, inputTopic)
	writer := getWriter(server, destTopic)
	defer reader.Close()
	defer writer.Close()
	for {
		fmt.Println("Reading input from input topic")
		rawInput, err := reader.ReadMessage(context.Background())
		fmt.Println("Errors:", err)
		if err != nil {
			break
		}
		stringInput := string(rawInput.Value)
		fmt.Println("Input body:", stringInput)
		// Convert one input Msg to some amount of output Msgs.
		fmt.Println("Converting msgs")
		outputMsgs := convertMsg(stringInput)
		// And send them
		fmt.Println("Writing msgs to dest topic:", outputMsgs)
		err = writeTextMsg(writer, outputMsgs)
		fmt.Println("Errors:", err)
		fmt.Println("")
	}
}

func getReader(server string, topic string) *kafka.Reader {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{server},
		Topic:     topic,
		Partition: 0,
	})
	return reader
}

func getWriter(server string, topic string) *kafka.Writer {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{server},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
	return writer
}

// Gets input json and returns array of maps
func convertMsg(inputString string) (outputMsgs []map[string]interface{}) {
	// Convert Msg from JSON to map.
	var inputMap map[string]interface{}
	json.Unmarshal([]byte(inputString), &inputMap)
	fullMsg := inputMap["Message"].(map[string]interface{})

	// Get data that isn't part of partitions.
	MsgWithoutPartitions := make(map[string]interface{})
	for key, value := range fullMsg {
		if key != "partitions" {
			MsgWithoutPartitions[key] = value
		}
	}

	// Msg with some partitions -> one Msg per partitions.
	partitions := fullMsg["partitions"].([]interface{})
	// Merge partitions with non-partitions data.
	for _, value := range partitions {
		for k, v := range MsgWithoutPartitions {
			value.(map[string]interface{})[k] = v
		}
	}
	// Plainify Msgs (make them just map[string]string).
	plainMsgs := make([]map[string]string, len(partitions))
	for i, Msg := range partitions {
		plainMsg := make(map[string]string)
		plainifyMsg(plainMsg, Msg.(map[string]interface{}))
		plainMsgs[i] = plainMsg
	}

	// Fill dest Msgs with real data.
	outputMsgs = make([]map[string]interface{}, len(partitions))
	for i, plainMsg := range plainMsgs {
		destMsg := make(map[string]interface{})
		copyMap(destMsg, destSignature)
		fillDestMsgWithData(destMsg, conversionMap, plainMsg)
		outputMsgs[i] = destMsg
	}

	return outputMsgs
}

// Fill Msg template with real data.
func fillDestMsgWithData(destMsg map[string]interface{}, conversionMap map[string]string, plainMsg map[string]string) {
	for fieldName, placeholder := range destMsg {
		switch placeholder.(type) {
		case map[string]interface{}:
			fillDestMsgWithData(destMsg[fieldName].(map[string]interface{}), conversionMap, plainMsg)
		default:
			if inputMsgKey, ok := conversionMap[placeholder.(string)]; ok {
				if valueToInsert, okok := plainMsg[inputMsgKey]; okok {
					destMsg[fieldName] = valueToInsert
					continue
				}
			}
		}
	}
}

// Function or deep nested maps copying.
func copyMap(destMap map[string]interface{}, sourceMap map[string]interface{}) {
	for fieldName, fieldValue := range sourceMap {
		switch sourceMap[fieldName].(type) {
		case map[string]interface{}:
			destMap[fieldName] = map[string]interface{}{}
			copyMap(destMap[fieldName].(map[string]interface{}), sourceMap[fieldName].(map[string]interface{}))
		default:
			destMap[fieldName] = fieldValue
		}
	}
}

func plainifyMsg(plainMsg map[string]string, Msg map[string]interface{}) {
	for fieldName, fieldValue := range Msg {
		varType := reflect.TypeOf(fieldValue).Kind()
		if varType == reflect.Map || varType == reflect.Slice {
			plainifyMsg(plainMsg, fieldValue.(map[string]interface{}))
		} else {
			if varType == reflect.Float64 {
				plainMsg[string(fieldName)] = strconv.Itoa(int(fieldValue.(float64)))
			} else {
				plainMsg[string(fieldName)] = fieldValue.(string)
			}
		}
	}
}

func writeTextMsg(writer *kafka.Writer, Msgs []map[string]interface{}) error {
	var err error
	for _, Msg := range Msgs {
		json, err := json.Marshal(Msg)
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
