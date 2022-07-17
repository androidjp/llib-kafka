package llibkafka

import "github.com/Shopify/sarama"

type KafkaOptions struct {
	addr                 []string
	cli                  sarama.Client
	sCfg                 *sarama.Config
	key                  string
	autoAck              bool
	topic                string
	consumeFromBeginning bool
}

type KafkaOption func(ops *KafkaOptions)

func WithAddr(addr ...string) KafkaOption {
	return func(ops *KafkaOptions) {
		ops.addr = append(ops.addr, addr...)
	}
}

func WithConsumeFromBeginning(fromBeginning bool) KafkaOption {
	return func(ops *KafkaOptions) {
		ops.consumeFromBeginning = fromBeginning
	}
}

func WithClient(cli sarama.Client) KafkaOption {
	return func(ops *KafkaOptions) {
		ops.cli = cli
	}
}

func WithCfg(cfg *sarama.Config) KafkaOption {
	return func(ops *KafkaOptions) {
		ops.sCfg = cfg
	}
}

func WithKey(key string) KafkaOption {
	return func(ops *KafkaOptions) {
		ops.key = key
	}
}

func WithAutoAck(autoACK bool) KafkaOption {
	return func(ops *KafkaOptions) {
		ops.autoAck = autoACK
	}
}

func WithTopic(topic string) KafkaOption {
	return func(ops *KafkaOptions) {
		ops.topic = topic
	}
}
