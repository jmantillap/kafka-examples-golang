package handlers

import (
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func Producer(kafkaConn *kafka.Conn) {
	var body string
	kafkaConn.SetWriteDeadline(time.Now().Add(10 * time.Second))

	for {
		fmt.Println("Escriba el mensaje a enviar")
		fmt.Scanf("%v", &body)
		msg := kafka.Message{
			Value: []byte(body),
		}

		_, err := kafkaConn.WriteMessages(
			msg,
		)

		if err != nil {
			log.Fatal("failed to write messages:", err)
		}
		/*if err := kafkaConn.Close(); err != nil {
			log.Fatal("failed to close writer:", err)
		}
		*/
	}

}
