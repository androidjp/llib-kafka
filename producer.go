package llib_kafka

import "github.com/Shopify/sarama"

type Producer struct {
	Opts *ProducerOptions
	SP   sarama.SyncProducer
	ASP  sarama.AsyncProducer
}

func NewProducer(ops ...ProducerOption) (*Producer, error) {
	opts := &ProducerOptions{
		KafkaOptions: &KafkaOptions{
			sCfg: sarama.NewConfig(),
		},
		ackType:               sarama.WaitForAll,           // 发送完数据需要leader和follow都确认
		partitionerChoiceFunc: sarama.NewRandomPartitioner, // 新选出一个partition
		successChannelReturn:  true,                        // 成功交付的消息将在success channel返回
	}

	for _, o := range ops {
		if err := o(opts); err != nil {
			return nil, err
		}
	}

	opts.sCfg.Producer.RequiredAcks = opts.ackType
	opts.sCfg.Producer.Partitioner = opts.partitionerChoiceFunc
	opts.sCfg.Producer.Return.Successes = opts.successChannelReturn

	return &Producer{
		Opts: opts,
	}, nil
}

func (p *Producer) Start() error {
	var (
		sp  sarama.SyncProducer
		asp sarama.AsyncProducer
		err error
	)

	if p.Opts.cli != nil {
		if p.Opts.isAsync {
			asp, err = sarama.NewAsyncProducerFromClient(p.Opts.cli)
		} else {
			sp, err = sarama.NewSyncProducerFromClient(p.Opts.cli)
		}
	} else {
		if p.Opts.isAsync {
			asp, err = sarama.NewAsyncProducer(p.Opts.addr, p.Opts.sCfg)
		} else {
			sp, err = sarama.NewSyncProducer(p.Opts.addr, p.Opts.sCfg)
		}
	}
	if err != nil {
		return err
	}
	p.SP = sp
	p.ASP = asp
	return nil
}

func (p *Producer) SendMessage(bodyStr string) error {
	return p.SendMessageToTopic(p.Opts.sendTopic, bodyStr)
}
func (p *Producer) SendMessageToTopic(topic, bodyStr string) error {
	msg := &sarama.ProducerMessage{}
	msg.Topic = topic
	msg.Value = sarama.StringEncoder(bodyStr)

	if p.SP != nil {
		_, _, err := p.SP.SendMessage(msg)
		return err
	}
	if p.ASP != nil {
		p.ASP.Input() <- msg
	}
	return nil
}

func (p *Producer) GetSuccessChannel() (<-chan *sarama.ProducerMessage, bool) {
	if p.ASP != nil {
		return p.ASP.Successes(), true
	}
	return nil, false
}

func (p *Producer) GetErrorChannel() (<-chan *sarama.ProducerError, bool) {
	if p.ASP != nil {
		return p.ASP.Errors(), true
	}
	return nil, false
}

func (p *Producer) Stop() error {
	if p.SP != nil {
		return p.SP.Close()
	}
	if p.ASP != nil {
		p.ASP.AsyncClose()
	}
	return nil
}
