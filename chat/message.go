package chat

import (
	"github.com/IBM/sarama"
)

type Message struct {
	Nickname string
	Msg      string
}

func PrepareMessage(topic string, bytes []byte) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		Topic:     topic,
		Value:     sarama.ByteEncoder(bytes),
		Partition: -1,
	}
}
