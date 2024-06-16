package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"kafka_Chat/chat"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var nickName string

var (
	addr  = []string{"localhost:29092"}
	topic = "chatni"
)

func main() {
	ctx := context.Background()
	//::: INTRODUCE yourself

	log.Println("Welcome to kafka-based_chat-app!!! Please, introduce yourself ...")
	_, err := fmt.Scanf("%s\n", &nickName)
	if err != nil {
		log.Fatalf("Wrong nickname input!!! ... %s", err)
	}
	//::: CONSUMER setup
	consumer := chat.NewConsumer(nickName)
	client, err := chat.NewConsumerGroup(addr, topic)
	if err != nil {
		panic("creating consumer ::: " + err.Error())

	}

	//::: PRODUCER setup
	producer, err := chat.NewProducer(addr, topic)
	if err != nil {
		panic("creating producer ::: " + err.Error())
	}

	//	::: PRODUCER sending messages
	go func() {
		for {
			//:: creating stdIN reader
			reader := bufio.NewReader(os.Stdin)
			in, err := reader.ReadString('\n')
			if err != nil {
				log.Fatalf("readString ::: %s", err)
			}
			//:: preparing message
			msg := chat.Message{
				Nickname: nickName,
				Msg:      in,
			}
			//:encoding message in bytes
			msgBytes, _ := json.Marshal(msg)
			//:ProducerMessage ready for kafka
			msgToSend := chat.PrepareMessage(topic, msgBytes)

			//::sending Message
			producer.Input() <- msgToSend
		}

	}()
	//	::: PRODUCER recieving success response
	go func() {
		for {

			select {
			case <-producer.Successes():
				log.Println("Succcess for msg!!! ")
			case error := <-producer.Errors():
				log.Println("...not success...", error)
			}
		}

	}()

	//::: CONSUMER start
	err = client.Consume(ctx, []string{topic}, consumer)
	if err != nil {
		panic("consume by cinsumer " + err.Error())
	}

	//::: DONE channel setup
	done := make(chan os.Signal)
	signal.Notify(done, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	<-done
}
