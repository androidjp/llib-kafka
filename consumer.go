package llibkafka

import (
	"errors"
	"github.com/Shopify/sarama"
)

type Consumer struct {
	Opts               *KafkaOptions
	C                  sarama.Consumer
	PartitionConsumers map[string][]sarama.PartitionConsumer
	closing            chan struct{}
}

func NewConsumer(co ...KafkaOption) (*Consumer, error) {
	var (
		c   sarama.Consumer
		err error
	)
	opts := &KafkaOptions{
		sCfg:    sarama.NewConfig(),
		autoAck: false,
	}
	for _, o := range co {
		o(opts)
	}
	opts.sCfg.Consumer.Return.Errors = true
	opts.sCfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	if opts.consumeFromBeginning {
		opts.sCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
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
		closing:            make(chan struct{}),
	}, nil
}

// GetAllPartitions 拿到某个topic下的所有分区
func (c *Consumer) GetAllPartitions(topic string) ([]int32, error) {
	return c.C.Partitions(topic)
}

func (c *Consumer) StartListen(consumeHandler ConsumeHandler, errHandler ConsumeErrHandler) error {
	topic := c.Opts.topic
	if len(topic) == 0 {
		return errors.New("topic is empty")
	}

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
			logErrorf("failed to start consumer for partition %d,err:%v\n", partition, err)
			return err
		}
		c.PartitionConsumers[topic] = append(c.PartitionConsumers[topic], pc)

		go func(pc sarama.PartitionConsumer) {
			<-c.closing
			pc.AsyncClose()
		}(pc)

		go func(consumer sarama.PartitionConsumer) {
			for msg := range consumer.Messages() {
				c.handleMessage(msg, topic, consumeHandler, errHandler)
			}
		}(pc)
	}
	return nil
}

func (c *Consumer) handleMessage(msg *sarama.ConsumerMessage, topic string, consumeHandler ConsumeHandler, errHandler ConsumeErrHandler) {
	logDebugf("Message claimed: value = %s, timestamp = %v, topic = %s",
		string(msg.Value), msg.Timestamp, msg.Topic)

	headers := make(map[string]string, len(msg.Headers))
	for _, val := range msg.Headers {
		headers[string(val.Key)] = string(val.Value)
	}

	m := &Message{
		Topic:       topic,
		Partition:   msg.Partition,
		Offset:      msg.Offset,
		ConsumerKey: "",
		Headers:     headers,
		Body:        msg.Value,
	}

	err := consumeHandler(m)
	if err == nil && c.Opts.autoAck {

	} else if err != nil {
		if errHandler != nil {
			errHandler(m, err)
		} else {
			logErrorf("[kafka]: subscriber error: %v", err)
		}
	}
}

func (c *Consumer) Stop() error {
	if c.closing != nil {
		close(c.closing)
		c.closing = nil
	}
	return nil
}
