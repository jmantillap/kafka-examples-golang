package handlers

import (
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func Consume(kafkaConn *kafka.Conn) {

	kafkaConn.SetReadDeadline(time.Now().Add(10 * time.Second))
	batch := kafkaConn.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max

	b := make([]byte, 10e3) // 10KB max per message

	for {
		n, err := batch.Read(b)
		if err != nil {
			break
		}
		fmt.Printf("received: %v", string(b[:n]))
		fmt.Println()
		//fmt.Println(string(b[:n]))
	}

	/*if err := batch.Close(); err != nil {
		log.Fatal("failed to close batch:", err)
	}*/

	if err := kafkaConn.Close(); err != nil {
		log.Fatal("failed to close connection:", err)
	}

	/*
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   []string{"127.0.0.1:9092"},
			Topic:     "my-topic",
			Partition: 0,
			GroupID:   group,
		})

		for {
			msg, err := r.ReadMessage(ctx)
			if err != nil {
				panic("could not read message " + err.Error())
			}
			fmt.Printf("%v received: %v", group, string(msg.Value))
			fmt.Println()
		}
	*/
}
