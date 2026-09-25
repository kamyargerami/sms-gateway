package kafka

import (
	"context"
	"encoding/json"
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

func (p *Producer) Produce(ctx context.Context, sms *domain.SMS) error {
	msgBytes, err := json.Marshal(sms)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:   []byte(sms.ID),
		Value: msgBytes,
	}

	if sms.IsExpress {
		return p.expressWriter.WriteMessages(ctx, msg)
	}
	return p.bulkWriter.WriteMessages(ctx, msg)
}

func (p *Producer) Close() {
	p.expressWriter.Close()
	p.bulkWriter.Close()
}
