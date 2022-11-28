package config

import (
	"context"

	"github.com/segmentio/kafka-go"
)

func GetKafkaConnection(kafkaURL, topic string) *kafka.Conn {

	conn, err := kafka.DialLeader(context.Background(), "tcp", kafkaURL, topic, 0)
	if err != nil {
		panic(err.Error())
	}

	/*return &kafka.Writer{
		Addr:  kafka.TCP(kafkaURL),
		Topic: topic,
		Balancer: &kafka.LeastBytes{},
	}
	*/

	return conn
}
