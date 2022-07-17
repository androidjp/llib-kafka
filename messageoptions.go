package llib_kafka

import "github.com/Shopify/sarama"

type MessageOption func(msg *sarama.ProducerMessage) error


