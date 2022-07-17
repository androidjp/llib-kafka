package main

import (
	"fmt"
	llibkafka "github.com/androidjp/llib-kafka"
	"time"
)

func main() {
	consumer1, err := llibkafka.NewConsumer(llibkafka.WithTopic("first_kafka_topic"), llibkafka.WithAddr("127.0.0.1:9092"))
	if err != nil {
		panic(err)
	}

	err = consumer1.StartListen(
		func(msg *llibkafka.Message) error {
			fmt.Println("msg received:", string(msg.Body))
			return nil
		}, func(msg *llibkafka.Message, err error) error {
			return nil
		},
	)

	time.Sleep(30 * time.Second)
	consumer1.Stop()
}
