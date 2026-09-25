package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sms/internal/config"
	"sms/internal/domain"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader             *kafka.Reader
	operator           domain.SMSOperator
	transactionManager domain.TransactionManager
	userRepo           domain.UserRepository
	smsRepo            domain.SMSRepository
	creditRepo         domain.CreditRepository
	cache              domain.CacheRepository
}

func NewConsumer(
	brokers []string,
	topic string,
	groupID string,
	transactionManager domain.TransactionManager,
	userRepo domain.UserRepository,
	smsRepo domain.SMSRepository,
	creditRepo domain.CreditRepository,
	cache domain.CacheRepository,
	operatorService domain.SMSOperator,
) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			GroupID:  groupID,
			Topic:    topic,
			MinBytes: 10e3,
			MaxBytes: 10e6,
		}),
		operator:           operatorService,
		transactionManager: transactionManager,
		userRepo:           userRepo,
		smsRepo:            smsRepo,
		creditRepo:         creditRepo,
		cache:              cache,
	}
}

func (consumer *Consumer) Start(goContext context.Context) {
	for {
		message, err := consumer.reader.FetchMessage(goContext)
		if err != nil {
			if goContext.Err() != nil {
				return // Context cancelled, exit gracefully
			}
			log.Printf("Error reading message: %v\n", err)
			continue
		}

		var sms domain.SMS
		if err := json.Unmarshal(message.Value, &sms); err != nil {
			log.Printf("Error unmarshalling sms: %v\n", err)
			if commitErr := consumer.reader.CommitMessages(goContext, message); commitErr != nil {
				log.Printf("Failed to commit unmarshallable message: %v\n", commitErr)
			}
			continue
		}

		sms.Status = "PENDING"
		cost := config.GetSMSCost()

		// 1. Transactional Insert (Unit of Work)
		err = consumer.transactionManager.WithTransaction(goContext, func(transactionCtx context.Context) error {
			if err := consumer.userRepo.UpdateBalance(transactionCtx, sms.UserID, -cost); err != nil {
				return err
			}
			if err := consumer.smsRepo.Create(transactionCtx, &sms); err != nil {
				return err
			}
			return consumer.creditRepo.Create(transactionCtx, sms.UserID, cost, "SMS_SENT")
		})

		if err != nil {
			if errors.Is(err, domain.ErrDuplicateRecord) {
				// Check if the previous worker finished processing it
				existingSMS, getErr := consumer.smsRepo.GetByID(goContext, sms.ID)
				if getErr == nil && existingSMS.Status != "PENDING" {
					if commitErr := consumer.reader.CommitMessages(goContext, message); commitErr != nil {
						log.Printf("Failed to commit duplicate message: %v\n", commitErr)
					}
					continue
				}
				// If it is PENDING, the previous worker crashed mid-flight.
				// We fall through and resume sending it to the operator!
			} else {
				log.Printf("Error inserting SMS to DB (will retry): %v\n", err)
				continue
			}
		}

		// 2. Simulate sending to operator
		success := consumer.operator.SendSMS(sms.ToNumber, sms.Text)

		status := "FAILED"
		if success {
			status = "DELIVERED"
		}

		// 3. Update DB to final status
		if err := consumer.smsRepo.UpdateStatus(goContext, sms.ID, status); err != nil {
			log.Printf("Error updating SMS status: %v\n", err)
		}

		// 4. Refund if failed
		if !success {
			err = consumer.transactionManager.WithTransaction(goContext, func(transactionCtx context.Context) error {
				if err := consumer.userRepo.UpdateBalance(transactionCtx, sms.UserID, cost); err != nil {
					return err
				}
				return consumer.creditRepo.Create(transactionCtx, sms.UserID, cost, "REFUND")
			})
			if err != nil {
				log.Printf("Error refunding MySQL: %v\n", err)
			} else {
				if err := consumer.cache.AddBalance(goContext, sms.UserID, cost); err != nil {
					log.Printf("Error refunding Redis: %v\n", err)
				}
			}
		}

		// 5. Commit offset
		if err := consumer.reader.CommitMessages(goContext, message); err != nil {
			log.Printf("Error committing message: %v\n", err)
		}

		fmt.Printf("Processed SMS: %s, Status: %s\n", sms.ID, status)
	}
}

func (consumer *Consumer) Close() error {
	return consumer.reader.Close()
}
