package main

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type orderPlacer struct {
	producer   *kafka.Producer
	topic      string
	deliverych chan kafka.Event
}

func NewOrderPlacer(p *kafka.Producer, topic string) *orderPlacer {
	return &orderPlacer{
		producer:   p,
		topic:      topic,
		deliverych: make(chan kafka.Event, 10000),
	}
}

func (op *orderPlacer) placeOrder(orderType string, size int) error {

	var (
		format  = fmt.Sprintf("%s - %d", orderType, size)
		payload = []byte(format)
	)

	err := op.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &op.topic, Partition: kafka.PartitionAny},
		Value:          payload,
	},
		op.deliverych,
	)

	if err != nil {
		fmt.Println(err)
	}

	<-op.deliverych
	fmt.Printf("Place order on the queue %s\n", format)

	return nil
}

func main() {

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "somthing",
		"acks":              "all",
	})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
	}

	op := NewOrderPlacer(p, "FOO")
	for i := 0; i < 1000; i++ {
		if err := op.placeOrder("Market: ", i+1); err != nil {
			fmt.Println(err)
		}
		time.Sleep(time.Second * 3)
	}

}
