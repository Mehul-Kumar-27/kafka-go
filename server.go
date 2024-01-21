package main

import (
	"log"
	"os"
	"kafkago/producer"
	"kafkago/consumer"
	"github.com/segmentio/kafka-go"
)

var loggger = log.New(os.Stdout, "kafka writer: ", 0)

func main() {
	//	ctx, cancle := context.WithTimeout(context.Background(), 10*time.Second)
	chn := make(chan *kafka.Conn)
	go consumer.CreateConsumer([]string{"localhost:9092"}, "topic1", loggger)
	go producer.PushMessages(loggger)
	// go func() {
	// 	conn, err := kafka.DialContext(ctx, "tcp", "localhost:9092")
	// 	if err != nil {
	// 		loggger.Fatalf("unable to connect to kafka: %v", err)
	// 	}else{
	// 		chn <- conn
	// 	}

	// 	defer conn.Close()
	// }()

	conn := <-chn

	loggger.Printf("Connected to kafka %d", conn.Broker().Port)

	//defer cancle()

	//defer conn.Close()

}
