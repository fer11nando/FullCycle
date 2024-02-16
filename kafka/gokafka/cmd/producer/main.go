package main

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	producer := NewKafkaProducer()
	Publish("mensagem", "teste", producer, nil)
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "gokafka_kafka_1:9092",
	}

	p, err := kafka.NewProducer(configMap)

	if err != nil {
		log.Println("Error producer: ", err)
	}

	return p
}

func Publish(message string, topic string, producer *kafka.Producer, key []byte) error {
	msg := &kafka.Message{
		Value: []byte(message),
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key: key,
	}

	err := producer.Produce(msg, nil)

	if err != nil {
		return err
	}

	return nil
}
