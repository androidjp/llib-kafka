package llibkafka

import "github.com/Shopify/sarama"

type ProducerOptions struct {
	*KafkaOptions
	sendTopic             string
	isAsync               bool
	ackType               sarama.RequiredAcks
	partitionerChoiceFunc func(topic string) sarama.Partitioner
	successChannelReturn  bool // 成功交付的消息将在success channel返回
	errorChannelReturn    bool // 失败的消息将在error channel返回
}

type ProducerOption func(options *ProducerOptions) error

func WithSendTopic(topic string) ProducerOption {
	return func(options *ProducerOptions) error {
		options.sendTopic = topic
		return nil
	}
}
func WithRequiredAckType(t sarama.RequiredAcks) ProducerOption {
	return func(options *ProducerOptions) error {
		options.ackType = t
		return nil
	}
}

func WithPartitionerChoiceFunc(p func(topic string) sarama.Partitioner) ProducerOption {
	return func(options *ProducerOptions) error {
		options.partitionerChoiceFunc = p
		return nil
	}
}

func WithSuccessChannelReturn(isOpen bool) ProducerOption {
	return func(options *ProducerOptions) error {
		options.successChannelReturn = isOpen
		return nil
	}
}

func WithIsAsync(isAsync bool) ProducerOption {
	return func(options *ProducerOptions) error {
		options.isAsync = isAsync
		return nil
	}
}

func WithKafkaOption(ops ...KafkaOption) ProducerOption {
	return func(options *ProducerOptions) error {
		for _, o := range ops {
			o(options.KafkaOptions)
		}
		return nil
	}
}
