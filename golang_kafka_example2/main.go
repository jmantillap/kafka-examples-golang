package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	ctx := context.Background()

	//go Producer(ctx)

	go Consumer(ctx)

	//go ConsumerCommit(ctx)

	time.Sleep(time.Minute * 2)
}

func ConsumerCommit(ctx context.Context) {
	fmt.Println("Se consume el metodo ConsumerCommit()")
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "topic-A", /*topic-A*/ /*my-topic*/
		/*GroupID: "test-consumer-group",*/
		/*MinBytes:       10e3,        // 10KB
		MaxBytes:       10e6,        // 10MB
		CommitInterval: time.Second, // flushes commits to Kafka every second*/
	})

	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			break
		}
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
		err1 := r.CommitMessages(ctx, m)
		if err1 != nil {
			log.Println(err1)
			fmt.Println("-")
		}
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}

func Consumer(ctx context.Context) {
	fmt.Println("Se ejecuta el metodo Consumer()")
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"localhost:9092"},
		Topic:          "topic-A",   /*topic-A*/
		MinBytes:       10e3,        // 10KB
		MaxBytes:       10e6,        // 10MB
		CommitInterval: time.Second, // flushes commits to Kafka every second
		/*Logger:         kafka.LoggerFunc(logf),
		ErrorLogger:    kafka.LoggerFunc(logf),*/
	})

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}

func Producer(ctx context.Context) {
	fmt.Println("Se consume el metodo Producer()")

	w := &kafka.Writer{
		Addr:                   kafka.TCP("localhost:9092"),
		Topic:                  "topic-A",
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}
	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("Key-A"),
			Value: []byte("Hello World!"),
		},
		kafka.Message{
			Key:   []byte("Key-B"),
			Value: []byte("One!"),
		},
		kafka.Message{
			Key:   []byte("Key-C"),
			Value: []byte("Two!"),
		},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}

func logf(msg string, a ...interface{}) {
	fmt.Printf(msg, a...)
	fmt.Println()
}
