package llib_kafka

import (
	"github.com/Shopify/sarama"
	"time"
)

type Message struct {

}



// NewMessage 构造一个消息
func NewMessage(topic string, opt ...MessageOption) (*sarama.ProducerMessage, error) {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Key:       nil,
		Value:     nil,
		Headers:   nil,
		Metadata:  nil,
		Offset:    0,
		Partition: 0,
		Timestamp: time.Time{},
	}

	for _, o := range opt {
		if err := o(msg); err != nil {
			return nil, err
		}
	}

	return msg, nil
}