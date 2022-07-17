package main

import (
	"fmt"
	llibkafka "github.com/androidjp/llib-kafka"
	"time"
)

func main() {
	consumerGroup1, err := llibkafka.NewConsumerGroup("queue01", llibkafka.WithTopic("first_kafka_topic"), llibkafka.WithAddr("127.0.0.1:9092"))
	if err != nil {
		panic(err)
	}

	err = consumerGroup1.StartListen(
		func(msg *llibkafka.Message) error {
			fmt.Println("msg received:", string(msg.Body))
			return nil
		}, func(msg *llibkafka.Message, err error) error {
			return nil
		},
	)

	time.Sleep(30 * time.Second)
	consumerGroup1.Stop()
}
