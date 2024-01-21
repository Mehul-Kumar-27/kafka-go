package consumer

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

func CreateConsumer(brokers []string, topic string, logger *log.Logger) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    topic,
		MaxBytes: 10e6,
	})

	reader.SetOffset(3)

	for {
		message, err := reader.ReadMessage(context.Background())
		if err != nil {
			logger.Printf("Error occured while reading the message %v", err)
			break
		} else {
			logger.Printf("Message received from kafka %v", string(message.Value))
		}
	}

	err := reader.Close()
	if err != nil {
		logger.Printf("Error occured while closing the reader %v", err)
	} else {
		logger.Println("Reader closed successfully")
	}

}
