package llibkafka

import (
	"context"
	"errors"
	"github.com/Shopify/sarama"
)

type ConsumerGroup struct {
	Opts    *KafkaOptions
	CG      sarama.ConsumerGroup
	closing chan struct{}
}

func NewConsumerGroup(groupID string, co ...KafkaOption) (*ConsumerGroup, error) {
	var (
		cg  sarama.ConsumerGroup
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
		cg, err = sarama.NewConsumerGroupFromClient(groupID, opts.cli)
	} else {
		cg, err = sarama.NewConsumerGroup(opts.addr, groupID, opts.sCfg)
	}

	if err != nil {
		logErrorf("NewConsumerGroup failed: %v", err)
		return nil, err
	}
	return &ConsumerGroup{
		Opts:    opts,
		CG:      cg,
		closing: make(chan struct{}),
	}, nil
}

func (cg *ConsumerGroup) StartListen(consumeHandler ConsumeHandler, errHandler ConsumeErrHandler) error {
	topic := cg.Opts.topic
	if len(topic) == 0 {
		return errors.New("topic is empty")
	}
	ctx := context.Background()
	topics := []string{topic}
	h := consumerGroupHandler{
		handler:    consumeHandler,
		errHandler: errHandler,
		autoAck:    cg.Opts.autoAck,
	}
	go func() {
		for {
			select {
			case err := <-cg.CG.Errors():
				if err != nil {
					logErrorf("consumer error:", err)
				}
			case <-cg.closing:
				cg.CG.Close()
			default:
				err := cg.CG.Consume(ctx, topics, &h)
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				} else if err != nil {
					logErrorf("CG.Consume failed: %v", err)
				}
			}
		}
	}()
	return nil
}

func (cg *ConsumerGroup) Stop() error {
	if cg.closing != nil {
		close(cg.closing)
		cg.closing = nil
	}
	return nil
}

// Consumer represents a Sarama t group t
type consumerGroupHandler struct {
	handler    ConsumeHandler
	errHandler ConsumeErrHandler
	autoAck    bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (t *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (t *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a t loop of ConsumerGroupClaim's Messages().
func (t *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		logDebugf("Message claimed: value = %s, timestamp = %v, topic = %s",
			string(message.Value), message.Timestamp, message.Topic)
		headers := make(map[string]string, len(message.Headers))
		for _, val := range message.Headers {
			headers[string(val.Key)] = string(val.Value)
		}
		msg := &Message{
			Headers: headers,
			Body:    message.Value,
		}
		err := t.handler(msg)
		if err == nil && t.autoAck {
			session.MarkMessage(message, "")
		} else if err != nil {
			eh := t.errHandler
			if eh != nil {
				eh(msg, err)
			} else {
				logErrorf("[kafka]: subscriber error: %v", err)
			}
		}
	}

	return nil
}
