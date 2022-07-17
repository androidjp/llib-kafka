# Golang基础库-kafka

kafka基础库，包含基础kafka生产者和消费者等常用能力。

## 使用指南

1首先，根据apollo或者其他配置，读取到config中：
比如，有下面这个config.json

```json
{
  "urls": [
    "127.0.0.1:9092"
  ],
  "sasl": {
    "password": "demoString",
    "enable": false,
    "user": "demoString"
  },
  "consumerGroups": {
    "consumer_group_AAAAAA": {
      "enable": false,
      "queueName": "queue_01",
      "topicName": "my_topic",
      "autoAck": false,
      "fromBeginning": false
    }
  },
  "tls": {
    "enable": false,
    "clientCer": "demoString",
    "clientKey": "demoString",
    "serverCer": "demoString",
    "insecureSkipVerify": false
  },
  "consumers": {
    "consumer_AAAAAA": {
      "enable": false,
      "queueName": "",
      "autoAck": false,
      "topicName": "my_topic",
      "fromBeginning": true
    }
  },
  "producers": {
    "producer_AAAAAA": {
      "enable": false,
      "routeKey": "",
      "topicName": "my_topic",
      "IsAsync": false
    }
  }
}
```
读取得到 Config对象

2. 然后，初始化Kafka管理器：
```go
// 1. 拿到配置
cfg := &Config{
        Urls: []string{"127.0.0.1:9092"},
        TLS: TLS{
            Enable: false,
        },
        SASL: SASL{
            Enable: false,
        },
    Producers: map[string]ProducerConfig{
        "producer_1": {
            Enable:    true,
            TopicName: "first",
            RouteKey:  "",
        },
    },
    Consumers: map[string]ConsumerConfig{},
    ConsumerGroups: map[string]ConsumerConfig{
        "consumer_1": {
            Enable:    true,
            TopicName: "first",
            QueueName: "queue1",
            AutoAck:   false,
        },
    },
}


// 2. 初始化kafka管理器
kf, err := NewKafka(cfg)
```

3. 启动某个消费者的监听
```go
// 启动消费者的监听
err := kf.Subscribe("consumer_1", 
	func(msg *Message) error {
        fmt.Println("msg received:", string(msg.Body))
        return nil
    }, 
    func(msg *Message, err error) error {
        return nil
    }
)
```

4. 使用某个生产者，发送消息
```go
err = kf.Publish("producer_1", &Message{
    Body: []byte("hello1"),
})
```

5. 结束
```go
err := kf.Close()
```
