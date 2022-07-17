package llibkafka

import (
	"fmt"
	"github.com/Shopify/sarama"
)

type Kafka struct {
	Cfg            *Config
	kafkaCfg       *sarama.Config
	consumers      map[string]*Consumer
	consumerGroups map[string]*ConsumerGroup
	producers      map[string]*Producer
}

type ConsumeHandler func(msg *Message) error
type ConsumeErrHandler func(msg *Message, err error) error

func (k *Kafka) init() error {
	var err error
	// 初始化 kafka 通用config
	if err = k.initKafkaCfg(); err != nil {
		return err
	}
	// 初始化所有consumer
	if err = k.initConsumers(); err != nil {
		return err
	}

	// 初始化所有consumerGroup
	if err = k.initConsumerGroups(); err != nil {
		return err
	}

	// 初始化所有producer
	if err = k.initProducers(); err != nil {
		return err
	}
	return nil
}

func (k *Kafka) initKafkaCfg() error {
	kafkaCfg := sarama.NewConfig()
	if k.Cfg.TLS.Enable {
		config, err := NewTLSConfig(k.Cfg.TLS.ClientCer, k.Cfg.TLS.ClientKey, k.Cfg.TLS.ServerCer)
		if err != nil {
			return err
		}
		kafkaCfg.Net.TLS.Enable = true
		kafkaCfg.Net.TLS.Config = config
	}
	if k.Cfg.SASL.Enable {
		kafkaCfg.Net.SASL.Enable = true
		kafkaCfg.Net.SASL.User = k.Cfg.SASL.User
		kafkaCfg.Net.SASL.Password = k.Cfg.SASL.Password
		kafkaCfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
		}
		kafkaCfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	}
	k.kafkaCfg = kafkaCfg
	return nil
}

func (k *Kafka) initConsumers() error {
	if len(k.Cfg.Consumers) == 0 {
		return nil
	}
	k.consumers = make(map[string]*Consumer, len(k.Cfg.Consumers))
	for key, c := range k.Cfg.Consumers {
		if !c.Enable {
			continue
		}
		consumer, err := NewConsumer(
			WithCfg(k.kafkaCfg),
			WithAddr(k.Cfg.Urls...),
			WithConsumeFromBeginning(c.FromBeginning),
			WithClient(nil),
			WithKey(key),
			WithAutoAck(c.AutoAck),
			WithTopic(c.TopicName),
		)
		if err != nil {
			return err
		}
		k.consumers[key] = consumer
	}
	return nil
}

func (k *Kafka) initConsumerGroups() error {
	if len(k.Cfg.ConsumerGroups) == 0 {
		return nil
	}
	k.consumerGroups = make(map[string]*ConsumerGroup, len(k.Cfg.ConsumerGroups))
	for key, c := range k.Cfg.ConsumerGroups {
		if !c.Enable {
			continue
		}
		cg, err := NewConsumerGroup(c.QueueName,
			WithCfg(k.kafkaCfg),
			WithAddr(k.Cfg.Urls...),
			WithConsumeFromBeginning(c.FromBeginning),
			WithClient(nil),
			WithKey(key),
			WithAutoAck(c.AutoAck),
			WithTopic(c.TopicName),
		)
		if err != nil {
			return err
		}
		k.consumerGroups[key] = cg
	}
	return nil
}

func (k *Kafka) initProducers() error {
	if len(k.Cfg.Producers) == 0 {
		return nil
	}
	k.producers = make(map[string]*Producer, len(k.Cfg.Producers))
	for key, c := range k.Cfg.Producers {
		if !c.Enable {
			continue
		}
		producer, err := NewProducer(
			WithSendTopic(c.TopicName),
			WithRequiredAckType(c.RequiredAckType),
			WithIsAsync(c.IsAsync),
			WithKafkaOption(
				WithCfg(k.kafkaCfg),
				WithAddr(k.Cfg.Urls...),
				WithClient(nil),
				WithKey(key),
				WithTopic(c.TopicName),
			),
		)
		if err != nil {
			return err
		}
		err = producer.Start()
		if err != nil {
			return err
		}
		k.producers[key] = producer
	}
	return nil
}

func NewKafka(cfg *Config) (*Kafka, error) {
	kf := new(Kafka)
	kf.Cfg = cfg

	err := kf.init()
	if err != nil {
		return nil, err
	}
	return kf, nil
}

func (k *Kafka) Publish(key string, msg *Message) error {
	if k.producers == nil {
		return fmt.Errorf("no such producer [%s]", key)
	}
	producer, ok := k.producers[key]
	if !ok {
		return fmt.Errorf("invalid producer key [%s]", key)
	}
	return producer.SendMessage(msg)
}

func (k *Kafka) Subscribe(key string, handler ConsumeHandler, errHandler ConsumeErrHandler) error {
	if k.consumers != nil {
		c, ok := k.consumers[key]
		if ok {
			err := c.StartListen(handler, errHandler)
			if err != nil {
				return err
			}
			return nil
		}
	}
	if k.consumerGroups != nil {
		c, ok := k.consumerGroups[key]
		if ok {
			err := c.StartListen(handler, errHandler)
			if err != nil {
				return err
			}
			return nil
		}
	}
	return fmt.Errorf("invalid producer key [%s]", key)
}

func (k *Kafka) Close() error {
	var err error
	if k.consumers != nil {
		for _, v := range k.consumers {
			if err = v.Stop(); err != nil {
				return err
			}
		}
	}

	if k.consumerGroups != nil {
		for _, v := range k.consumerGroups {
			if err = v.Stop(); err != nil {
				return err
			}
		}
	}

	if k.producers != nil {
		for _, v := range k.producers {
			if err = v.Stop(); err != nil {
				return err
			}
		}
	}

	return nil
}
