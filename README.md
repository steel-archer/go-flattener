# go-flattener

*Our files:*
* flatteners.json - our config file
* flattener.go - main program
* example_producer.go and example_consumer.go - for testing purposes, first creates one message and latter works and reads messages from the specified topic.

*For this task we should make some assumptions about flatteners.json structure (and input/outpus messages structure as well):*
* Top element of flatteners.json is an array with 1 element (program should be extended if we want to work with multiple flattener scenarios).
* This element contains sub-elements "inputTopic", "destinationTopic", "graph", "destinationMessage".
* "graph" contains "Message".
* "Message" contains "partitions" what is an array.
* Except "partitions" "Message" can contain another data that is common fot all partitions.
* Elements of "partitions" can have any structure.

*Example of flattener.go output:*
```
$ go run flattener.go 
Initializing kafka reader and writer
Reading messages from an input topic
Errors: <nil>
Input body: {"Action":"something","Message":{"partitions":[{"name":"c:","driveType":3,"metric":{"usedSpaceBytes":342734824,"totalSpaceBytes":34273482423}},{"name":"d:","driveType":3,"metric":{"usedSpaceBytes":942734824,"totalSpaceBytes":904273482423}}],"createAtTimeUTC":"2017-08-07T08:38:43.3059476Z"}}
Converting msgs
Output msgs
[map[data:map[createAtTimeUTC:2017-08-07T08:38:43.3059476Z driveType:3 name:c: totalSpaceBytes:34273482423 usedSpaceBytes:342734824]] map[data:map[createAtTimeUTC:2017-08-07T08:38:43.3059476Z driveType:3 name:d: totalSpaceBytes:904273482423 usedSpaceBytes:942734824]]]
Writing messages to a destination topic
Errors: <nil>
```
