package main

import (
	. "jmantillap/kafka-example/config"
	. "jmantillap/kafka-example/handlers"
	"time"
)

func main() {

	conn := GetKafkaConnection("127.0.0.1:9092", "my-topic")

	go Producer(conn)

	go Consume(conn)

	time.Sleep(time.Minute * 1)

}
