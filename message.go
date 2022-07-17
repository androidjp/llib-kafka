package llibkafka

type Message struct {
	Topic       string
	Partition   int32
	Offset      int64
	ConsumerKey string
	Headers     map[string]string
	Body        []byte
}
