package main

import (
	llibkafka "llib-kafka"
	"time"
)

func main() {
	producer, err := llibkafka.NewProducer(llibkafka.WithKafkaOption(llibkafka.WithAddr("127.0.0.1:9092")))
	if err != nil {
		panic(err)
	}

	err = producer.Start()
	if err != nil {
		panic(err)
	}

	err = producer.SendMessageToTopic("first_kafka_topic", "hello!!!!!!!!!!!")
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Second)

}
