package kafka

import (
	"context"
	"encoding/json"
	"log"
	"sms/internal/domain"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	expressWriter *kafka.Writer
	bulkWriter    *kafka.Writer
}

func NewProducer(brokers []string) *Producer {
	return &Producer{
		expressWriter: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        "sms_express",
			Balancer:     &kafka.RoundRobin{},
			BatchTimeout: 10 * time.Millisecond,
		},
		bulkWriter: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        "sms_bulk",
			Balancer:     &kafka.RoundRobin{},
			BatchTimeout: 10 * time.Millisecond,
		},
	}
}

func (producer *Producer) Produce(goContext context.Context, sms *domain.SMS) error {
	messageBytes, err := json.Marshal(sms)
	if err != nil {
		return err
	}

	message := kafka.Message{
		Key:   []byte(sms.ID),
		Value: messageBytes,
	}

	if sms.IsExpress {
		return producer.expressWriter.WriteMessages(goContext, message)
	}
	return producer.bulkWriter.WriteMessages(goContext, message)
}

func (producer *Producer) Close() {
	if err := producer.expressWriter.Close(); err != nil {
		log.Printf("Error closing express writer: %v\n", err)
	}
	if err := producer.bulkWriter.Close(); err != nil {
		log.Printf("Error closing bulk writer: %v\n", err)
	}
}
