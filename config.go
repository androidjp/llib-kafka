package llibkafka

import "github.com/Shopify/sarama"

type Config struct {
	Urls           []string                  `json:"urls" yaml:"urls"`
	TLS            TLS                       `json:"tls" yaml:"tls"`
	SASL           SASL                      `json:"sasl" yaml:"sasl"`
	Producers      map[string]ProducerConfig `json:"producers" yaml:"producers"`
	Consumers      map[string]ConsumerConfig `json:"consumers" yaml:"consumers"`
	ConsumerGroups map[string]ConsumerConfig `json:"consumerGroups" yaml:"consumerGroups"`
}

type SASL struct {
	Enable   bool   `json:"enable" yaml:"enable"`
	User     string `json:"user" yaml:"user"`
	Password string `json:"password" yaml:"password"`
}

type TLS struct {
	Enable             bool   `json:"enable" yaml:"enable"`
	InsecureSkipVerify bool   `json:"insecureSkipVerify" yaml:"insecureSkipVerify"`
	ClientCer          string `json:"clientCer" yaml:"clientCer"`
	ClientKey          string `json:"clientKey" yaml:"clientKey"`
	ServerCer          string `json:"serverCer" yaml:"serverCer"`
}

type ProducerConfig struct {
	Enable          bool                `json:"enable" yaml:"enable"`
	TopicName       string              `json:"topicName" yaml:"topicName"`
	RouteKey        string              `json:"routeKey" yaml:"routeKey"`
	IsAsync         bool                `json:"IsAsync" yaml:"isAsync"`
	RequiredAckType sarama.RequiredAcks `json:"requiredAckType" yaml:"requiredAckType"`
}

type ConsumerConfig struct {
	Enable        bool   `json:"enable" yaml:"enable"`
	TopicName     string `json:"topicName" yaml:"topicName"`
	QueueName     string `json:"queueName" yaml:"queueName"`
	AutoAck       bool   `json:"autoAck" yaml:"autoAck"`
	FromBeginning bool   `json:"fromBeginning" yaml:"fromBeginning"`
}
