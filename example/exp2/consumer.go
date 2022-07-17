package main

import (
	"fmt"
	llibkafka "github.com/androidjp/llib-kafka"
	"time"
)

func main() {
	// 1. 初始化kafka管理器
	kf, err := llibkafka.NewKafka(&llibkafka.Config{
		Urls: []string{"192.168.200.130:9092"},
		TLS: llibkafka.TLS{
			Enable: false,
		},
		SASL: llibkafka.SASL{
			Enable: false,
		},
		ConsumerGroups: map[string]llibkafka.ConsumerConfig{
			"consumer_1": {
				Enable:        true,
				TopicName:     "mytest",
				QueueName:     "queue1",
				AutoAck:       false,
				FromBeginning: true,
			},
		},
	})

	if err != nil {
		panic(err)
	}

	// 2. 启动消费者的监听
	err = kf.Subscribe("consumer_1", func(msg *llibkafka.Message) error {
		fmt.Println("msg received:", string(msg.Body))
		return nil
	}, func(msg *llibkafka.Message, err error) error {
		return nil
	})

	if err != nil {
		panic(err)
	}

	// 4. 结束
	time.Sleep(30 * time.Second)
	err = kf.Close()

	if err != nil {
		panic(err)
	}
}
