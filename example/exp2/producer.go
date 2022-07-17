package main

import llibkafka "github.com/androidjp/llib-kafka"

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
		Producers: map[string]llibkafka.ProducerConfig{
			"producer_1": {
				Enable:    true,
				TopicName: "mytest",
				RouteKey:  "",
			},
		},
		Consumers:      map[string]llibkafka.ConsumerConfig{},
		ConsumerGroups: map[string]llibkafka.ConsumerConfig{},
	})

	if err != nil {
		panic(err)
	}

	// 2. 生产消息
	err = kf.Publish("producer_1", &llibkafka.Message{
		Body: []byte("hello1"),
	})
	if err != nil {
		panic(err)
	}
	err = kf.Publish("producer_1", &llibkafka.Message{
		Body: []byte("hello2"),
	})
	if err != nil {
		panic(err)
	}
	err = kf.Publish("producer_1", &llibkafka.Message{
		Body: []byte("hello3"),
	})
	if err != nil {
		panic(err)
	}

	// 4. 结束
	err = kf.Close()
	if err != nil {
		panic(err)
	}
}
