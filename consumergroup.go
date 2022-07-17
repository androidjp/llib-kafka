package llib_kafka

import "github.com/Shopify/sarama"

type ConsumerGroup struct {
	Opts *KafkaOptions
	CG   sarama.ConsumerGroup
}

func NewConsumerGroup(groupID string, co ...KafkaOption) (*ConsumerGroup, error) {
	var (
		cg  sarama.ConsumerGroup
		err error
	)
	opts := &KafkaOptions{
		sCfg: sarama.NewConfig(),
	}

	for _, o := range co {
		o(opts)
	}

	if opts.cli != nil {
		cg, err = sarama.NewConsumerGroupFromClient(groupID, opts.cli)
	} else {
		cg, err = sarama.NewConsumerGroup(opts.addr, groupID, opts.sCfg)
	}

	if err != nil {
		return nil, err
	}
	return &ConsumerGroup{
		Opts: opts,
		CG:   cg,
	}, nil
}

