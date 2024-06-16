package chat

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/hashicorp/go-uuid"
	"log"
	"strings"
)

func NewConsumerGroup(brokers []string, topic string) (sarama.ConsumerGroup, error) {
	cfg := sarama.NewConfig()
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}

	return sarama.NewConsumerGroup(brokers, generateID(), cfg)
}

func generateID() string {
	id, _ := uuid.GenerateUUID()
	return string(id)

}

type Consumer struct {
	nickname string
}

func (c Consumer) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (c Consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil

}

func (c Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msgRcvd := range claim.Messages() {

		var msg Message
		var tabRepeater int
		err := json.Unmarshal(msgRcvd.Value, &msg)
		if err != nil {
			log.Println("... couldn't unmarshal sms ..." + err.Error())
		}
		if msg.Nickname == c.nickname {
			tabRepeater = 3
		} else {
			tabRepeater = 0
		}
		fmt.Printf(strings.Repeat("\t", tabRepeater)+"%s -> %s \n", msg.Nickname, msg.Msg)
	}
	return nil
}

func NewConsumer(s string) *Consumer {
	return &Consumer{s}
}
