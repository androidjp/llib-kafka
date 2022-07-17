package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	llibkafka "llib-kafka"
	"time"
)

func main() {
	// 消费命令：kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_kafka_topic --from-beginning

	consumer1, err := llibkafka.NewConsumer(llibkafka.WithAddr("127.0.0.1:9092"))
	if err != nil {
		panic(err)
	}

	err = consumer1.StartListen("first_kafka_topic", func(msg *sarama.ConsumerMessage) {
		fmt.Println("msg received:", string(msg.Value))
	})
	//if err != nil {
	//	panic(err)
	//}

	//llibkafka.Consume()

	time.Sleep(30 * time.Second)
}
