package main

import (
	"fmt"
	llibkafka "github.com/androidjp/llib-kafka"
)

func main() {
	// 1. 初始化kafka管理器
	kf, err := llibkafka.NewKafka(&llibkafka.Config{
		Urls: []string{"127.0.0.1:9092"},
		TLS: llibkafka.TLS{
			Enable: false,
		},
		SASL: llibkafka.SASL{
			Enable: false,
		},
		Producers: map[string]llibkafka.ProducerConfig{
			"producer_1": {
				Enable:    true,
				TopicName: "first",
				RouteKey:  "",
			},
		},
		Consumers: map[string]llibkafka.ConsumerConfig{},
		ConsumerGroups: map[string]llibkafka.ConsumerConfig{
			"consumer_1": {
				Enable:    true,
				TopicName: "first",
				QueueName: "queue1",
				AutoAck:   false,
			},
		},
	})

	fmt.Println(err)

	// 2. 启动消费者的监听
	_ = kf.Subscribe("consumer_1", func(msg *llibkafka.Message) error {
		fmt.Println("msg received:", string(msg.Body))
		return nil
	}, func(msg *llibkafka.Message, err error) error {
		return nil
	})

	// 3. 生产消息
	_ = kf.Publish("producer_1", &llibkafka.Message{
		Body: []byte("hello"),
	})

	// 4. 结束
	_ = kf.Close()
}
