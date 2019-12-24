# go-flattener
* flatteners.json - our config file
* flattener.go - main program
* test_producer.go and test_consumer.go - for testing purposes, first creates one message and latter works and reads messages from the specified topic.

Example of flattener.go output:
```
$ go run flattener.go 
Reading input from input topic
Errors: <nil>
Input body: {"Action":"something","Message":{"partitions":[{"name":"c:","driveType":3,"metric":{"usedSpaceBytes":342734824,"totalSpaceBytes":34273482423}},{"name":"d:","driveType":3,"metric":{"usedSpaceBytes":942734824,"totalSpaceBytes":904273482423}}],"createAtTimeUTC":"2017-08-07T08:38:43.3059476Z"}}
Converting msgs
Writing msgs to dest topic: [map[data:map[createAtTimeUTC:2017-08-07T08:38:43.3059476Z driveType:3 name:c: totalSpaceBytes:34273482423 usedSpaceBytes:342734824]] map[data:map[createAtTimeUTC:2017-08-07T08:38:43.3059476Z driveType:3 name:d: totalSpaceBytes:904273482423 usedSpaceBytes:942734824]]]
Jsoned output: {"data":{"createAtTimeUTC":"2017-08-07T08:38:43.3059476Z","driveType":"3","name":"c:","totalSpaceBytes":"34273482423","usedSpaceBytes":"342734824"}}
Jsoned output: {"data":{"createAtTimeUTC":"2017-08-07T08:38:43.3059476Z","driveType":"3","name":"d:","totalSpaceBytes":"904273482423","usedSpaceBytes":"942734824"}}
Errors: <nil>
```
