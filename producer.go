package llibkafka

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
		errorChannelReturn:    true,                        // 失败的消息将在error channel返回
	}

	for _, o := range ops {
		if err := o(opts); err != nil {
			return nil, err
		}
	}

	opts.sCfg.Producer.RequiredAcks = opts.ackType
	opts.sCfg.Producer.Partitioner = opts.partitionerChoiceFunc
	opts.sCfg.Producer.Return.Successes = opts.successChannelReturn
	opts.sCfg.Producer.Return.Errors = opts.errorChannelReturn

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
		logErrorf("Producer start failed: %v", err)
		return err
	}
	p.SP = sp
	p.ASP = asp
	return nil
}

func (p *Producer) SendMessage(msg *Message) error {
	return p.SendMessageToTopic(p.Opts.sendTopic, msg)
}
func (p *Producer) SendMessageToTopic(topic string, msg *Message) error {
	sMsg := &sarama.ProducerMessage{}
	sMsg.Topic = topic
	sMsg.Headers = make([]sarama.RecordHeader, 0)
	for k, v := range msg.Headers {
		sMsg.Headers = append(sMsg.Headers, sarama.RecordHeader{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}
	sMsg.Value = sarama.StringEncoder(msg.Body)

	if p.SP != nil {
		_, _, err := p.SP.SendMessage(sMsg)
		return err
	}
	if p.ASP != nil {
		p.ASP.Input() <- sMsg
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
