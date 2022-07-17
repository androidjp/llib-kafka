package main

import (
	llibkafka "github.com/androidjp/llib-kafka"
	"time"
)

func main() {
	producer, err := llibkafka.NewProducer(llibkafka.WithSendTopic("first_kafka_topic"), llibkafka.WithKafkaOption(llibkafka.WithAddr("127.0.0.1:9092")))
	if err != nil {
		panic(err)
	}

	err = producer.Start()
	if err != nil {
		panic(err)
	}

	err = producer.SendMessageToTopic("first_kafka_topic", &llibkafka.Message{
		Body: []byte("hello!!!!!!!!!!!"),
	})
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Second)
	producer.Stop()
}
