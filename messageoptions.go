package llibkafka

import "github.com/Shopify/sarama"

type MessageOption func(msg *sarama.ProducerMessage) error
