package producer

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

var writer *kafka.Writer

func CreateWriter(kafkaBrokers []string, topic string, logger *log.Logger) (*kafka.Writer, error) {
	var err error
	writer = &kafka.Writer{
		Addr:         kafka.TCP(kafkaBrokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		Async:        false,
		ReadTimeout:  time.Second * 10,
		WriteTimeout: time.Second * 10,
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				logger.Fatalf("Error occured while writing to the kafka %s", err)
			} else {
				logger.Println(messages)
			}
		},
	}

	return writer, err
}

func PushMessages(logger *log.Logger) {
	writer, err := CreateWriter([]string{"localhost:9092"}, "topic1", logger)
	if err != nil {
		logger.Panicln("Error while creting the writer")
	}

	logger.Println("Writer Created Successfully")

	for i := 0; i < 10; i++ {
		m := kafka.Message{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte("Hello World " + strconv.Itoa(i)),
		}
		err := writer.WriteMessages(context.Background(), m)
		if err != nil {
			logger.Panicf("Unable to push messages to kafka %v", err)
		}
		logger.Printf("Pushed message %s", string(m.Value))

		time.Sleep(time.Second * 2)
	}

	er := writer.Close()
	if er != nil {
		logger.Panicf("Unable to close the connection between the kafka and the producer : %v", er)
	} else {
		logger.Println("Connection closed successfully")
	}

}
