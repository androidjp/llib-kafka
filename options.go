package llib_kafka

import "github.com/Shopify/sarama"


type KafkaOptions struct {
	addr []string
	cli  sarama.Client
	sCfg *sarama.Config
}

type KafkaOption func(ops *KafkaOptions)

func WithAddr(addr ...string) KafkaOption {
	return func(ops *KafkaOptions) {
		ops.addr = append(ops.addr, addr...)
	}
}

func WithClient(cli sarama.Client) KafkaOption {
	return func(ops *KafkaOptions) {
		ops.cli = cli
	}
}