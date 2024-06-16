package chat

import "github.com/IBM/sarama"

func NewProducer(brokers []string, topic string) (sarama.AsyncProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Partitioner = sarama.NewCustomPartitioner()
	cfg.Producer.Return.Successes = true

	return sarama.NewAsyncProducer(brokers, cfg)
}
