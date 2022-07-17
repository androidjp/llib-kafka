package llib_kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
)

type Consumer struct {
	Opts               *KafkaOptions
	C                  sarama.Consumer
	PartitionConsumers map[string][]sarama.PartitionConsumer
}

func NewConsumer(co ...KafkaOption) (*Consumer, error) {
	var (
		c   sarama.Consumer
		err error
	)
	opts := &KafkaOptions{
		sCfg: sarama.NewConfig(),
	}

	for _, o := range co {
		o(opts)
	}

	if opts.cli != nil {
		c, err = sarama.NewConsumerFromClient(opts.cli)
	} else {
		c, err = sarama.NewConsumer(opts.addr, opts.sCfg)
	}

	if err != nil {
		return nil, err
	}
	return &Consumer{
		Opts:               opts,
		C:                  c,
		PartitionConsumers: make(map[string][]sarama.PartitionConsumer),
	}, nil
}

// GetAllPartitions 拿到某个topic下的所有分区
func (c *Consumer) GetAllPartitions(topic string) ([]int32, error) {
	return c.C.Partitions(topic)
}

type ConsumeExecFunc func(msg *sarama.ConsumerMessage)

func (c *Consumer) StartListen(topic string, execFunc ConsumeExecFunc) error {
	pList, err := c.GetAllPartitions(topic)
	if err != nil {
		return err
	}
	if len(c.PartitionConsumers[topic]) == 0 {
		c.PartitionConsumers[topic] = make([]sarama.PartitionConsumer, 0)
	}

	for partition := range pList {
		// 针对每个分区创建一个对应的分区消费者
		pc, err := c.C.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("failed to start consumer for partition %d,err:%v\n", partition, err)
			return err
		}
		c.PartitionConsumers[topic] = append(c.PartitionConsumers[topic], pc)

		go func(consumer sarama.PartitionConsumer) {
			for msg := range consumer.Messages() {
				execFunc(msg)
			}
		}(pc)
		//for msg := range pc.Messages() {
		//	execFunc(msg)
		//}
	}
	return nil
}

func Consume() {
	consumer, err := sarama.NewConsumer([]string{"127.0.0.1:9092"}, nil)
	if err != nil {
		fmt.Printf("kafka connnet failed, error[%v]", err.Error())
		return
	}
	//defer consumer.Close()

	partitions, err := consumer.Partitions("first_kafka_topic")
	if err != nil {
		fmt.Printf("get topic failed, error[%v]", err.Error())
		return
	}
	for _, p := range partitions {
		partitionConsumer, err := consumer.ConsumePartition("first_kafka_topic", p, sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("get partition consumer failed, error[%v]", err.Error())
			continue
		}
		go func(pc sarama.PartitionConsumer) {
			for message := range pc.Messages() {
				fmt.Printf("message:[%v], key:[%v], offset:[%v]\n", string(message.Value), string(message.Key), string(message.Offset))
				//this.MessageQueue <- string(message.Value)
			}
		}(partitionConsumer)
	}
}

func (c *Consumer) StopAllListen() error {
	for _, pcList := range c.PartitionConsumers {
		for i, _ := range pcList {
			pcList[i].AsyncClose()
		}
	}
	return nil
}
