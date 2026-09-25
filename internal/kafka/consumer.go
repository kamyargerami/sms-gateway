package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sms/internal/config"
	"sms/internal/domain"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader   *kafka.Reader
	operator domain.SMSOperator
	dbRepo   domain.DatabaseRepository
	cache    domain.CacheRepository
}

func NewConsumer(brokers []string, topic string, groupID string, dbRepo domain.DatabaseRepository, cache domain.CacheRepository, op domain.SMSOperator) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			GroupID:  groupID,
			Topic:    topic,
			MinBytes: 10e3, // 10KB
			MaxBytes: 10e6, // 10MB
		}),
		operator: op,
		dbRepo:   dbRepo,
		cache:    cache,
	}
}

func (c *Consumer) Start(ctx context.Context) {
	for {
		m, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // Context canceled
			}
			log.Printf("Error fetching message: %v\n", err)
			continue
		}

		var sms domain.SMS
		if err := json.Unmarshal(m.Value, &sms); err != nil {
			log.Printf("Error unmarshalling sms: %v\n", err)
			c.reader.CommitMessages(ctx, m)
			continue
		}

		// 1. Idempotency Check: Insert into DB first.
		// If it fails with Duplicate Entry, it means this message was already processed.
		sms.Status = "PENDING"
		if err := c.dbRepo.CreateSMS(&sms); err != nil {
			// MySQL Error 1062 is Duplicate entry
			if err.Error() != "" && (len(err.Error()) > 10 && err.Error()[:10] == "Error 1062") {
				c.reader.CommitMessages(ctx, m)
				continue
			}
			// If it's another DB error, we skip and DON'T commit, so Kafka will retry it later.
			log.Printf("Error inserting SMS to DB (will retry): %v\n", err)
			continue
		}

		// 2. Simulate sending to operator
		success := c.operator.SendSMS(sms.ToNumber, sms.Text)

		status := "FAILED"
		if success {
			status = "DELIVERED"
		}

		// 3. Update DB to final status
		if err := c.dbRepo.UpdateSMSStatus(sms.ID, status); err != nil {
			log.Printf("Error updating SMS status: %v\n", err)
		}

		// 4. Refund if failed
		if !success {
			cost := config.GetSMSCost()
			if err := c.dbRepo.RefundUser(sms.UserID, cost); err != nil {
				log.Printf("Error refunding MySQL: %v\n", err)
			}
			if err := c.cache.AddBalance(ctx, sms.UserID, cost); err != nil {
				log.Printf("Error refunding Redis: %v\n", err)
			}
		}

		// 5. Commit offset
		if err := c.reader.CommitMessages(ctx, m); err != nil {
			log.Printf("Error committing message: %v\n", err)
		}

		fmt.Printf("Processed SMS: %s, Status: %s\n", sms.ID, status)
	}
}

func (c *Consumer) Close() {
	c.reader.Close()
}
