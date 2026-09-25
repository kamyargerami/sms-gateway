package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"sms/internal/config"
	"sms/internal/domain"

	"github.com/segmentio/kafka-go"
)

// messageReader is the subset of *kafka.Reader the consumer uses (allows testing).
type messageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type Consumer struct {
	reader             messageReader
	operator           domain.SMSOperator
	transactionManager domain.TransactionManager
	userRepo           domain.UserRepository
	smsRepo            domain.SMSRepository
	creditRepo         domain.CreditRepository
	cache              domain.CacheRepository

	retryBackoff time.Duration
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
			Brokers: brokers,
			GroupID: groupID,
			Topic:   topic,
			// MinBytes=1 + short MaxWait: return as soon as any message is available.
			// The previous MinBytes=10KB made the broker hold fetches for up to
			// MaxWait (10s default) under low traffic, which breaks the express SLA.
			MinBytes: 1,
			MaxBytes: 10e6,
			MaxWait:  100 * time.Millisecond,
		}),
		operator:           operatorService,
		transactionManager: transactionManager,
		userRepo:           userRepo,
		smsRepo:            smsRepo,
		creditRepo:         creditRepo,
		cache:              cache,
		retryBackoff:       500 * time.Millisecond,
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
			if !sleepCtx(goContext, consumer.retryBackoff) {
				return
			}
			continue
		}

		if err := consumer.processMessage(goContext, message); err != nil {
			// Only reached on shutdown. Do NOT commit: the message is redelivered
			// to the next consumer that owns this partition.
			log.Printf("Stopped before finishing message at offset %d: %v\n", message.Offset, err)
			return
		}

		// Commit with a detached context so a shutdown signal doesn't drop a
		// commit for a message that has been fully processed.
		commitCtx, cancel := context.WithTimeout(context.WithoutCancel(goContext), 5*time.Second)
		if err := consumer.reader.CommitMessages(commitCtx, message); err != nil {
			log.Printf("Error committing message: %v\n", err)
		}
		cancel()
	}
}

// processMessage handles one SMS end-to-end. It returns a non-nil error only when
// the consumer is shutting down before the message could be fully processed; any
// other failure is retried in place.
//
// Important: kafka-go's FetchMessage never re-delivers a message that was skipped
// with `continue`, and committing a later offset implicitly commits every earlier
// one. So a message must never be skipped on a transient error — it has to be
// retried here, otherwise it is silently lost (and the user's credit with it).
func (consumer *Consumer) processMessage(goContext context.Context, message kafka.Message) error {
	var sms domain.SMS
	if err := json.Unmarshal(message.Value, &sms); err != nil {
		log.Printf("Error unmarshalling sms (dropping poison message): %v\n", err)
		return nil
	}

	sms.Status = domain.StatusPending
	cost := config.GetSMSCost()

	// 1. Transactional debit + insert (Unit of Work). The MySQL debit is guarded
	// (balance >= cost), so even if Redis let a request through with a stale
	// balance, the source of truth never goes negative and no free SMS is sent.
	alreadyProcessed := false
	insufficient := false
	err := consumer.retry(goContext, func(ctx context.Context) error {
		txErr := consumer.transactionManager.WithTransaction(ctx, func(transactionCtx context.Context) error {
			if err := consumer.userRepo.DeductBalance(transactionCtx, sms.UserID, cost); err != nil {
				return err
			}
			if err := consumer.smsRepo.Create(transactionCtx, &sms); err != nil {
				return err
			}
			return consumer.creditRepo.Create(transactionCtx, sms.UserID, cost, domain.CreditSMSSent)
		})

		switch {
		case txErr == nil:
			return nil
		case errors.Is(txErr, domain.ErrDuplicateRecord):
			// Redelivery. The rollback above undid the second debit.
			existingSMS, getErr := consumer.smsRepo.GetByID(ctx, sms.ID)
			if getErr != nil {
				return getErr
			}
			if existingSMS.Status != domain.StatusPending {
				alreadyProcessed = true
			}
			// If still PENDING, the previous worker crashed mid-flight: resume sending.
			return nil
		case errors.Is(txErr, domain.ErrInsufficientBalance), errors.Is(txErr, domain.ErrUserNotFound):
			insufficient = true
			return nil
		default:
			return txErr
		}
	})
	if err != nil {
		return err
	}
	if alreadyProcessed {
		return nil
	}

	if insufficient {
		// Nothing was debited in MySQL. But this may be a redelivery of an SMS that
		// WAS debited by a worker that crashed before sending (the guarded debit runs
		// before the INSERT, so it can fail first). Inserting the record tells us.
		sms.Status = domain.StatusFailed
		resume := false
		err := consumer.retry(goContext, func(ctx context.Context) error {
			createErr := consumer.smsRepo.Create(ctx, &sms)
			if createErr == nil {
				return nil
			}
			if !errors.Is(createErr, domain.ErrDuplicateRecord) {
				return createErr
			}
			existingSMS, getErr := consumer.smsRepo.GetByID(ctx, sms.ID)
			if getErr != nil {
				return getErr
			}
			resume = existingSMS.Status == domain.StatusPending
			return nil
		})
		if err != nil {
			return err
		}
		sms.Status = domain.StatusPending

		if !resume {
			// Rejected. Redis let this through, so it is out of sync: drop the cached
			// value and let the next request reload it from MySQL.
			log.Printf("Rejected SMS %s: insufficient balance for user %d\n", sms.ID, sms.UserID)
			if err := consumer.cache.InvalidateBalance(context.WithoutCancel(goContext), sms.UserID); err != nil {
				log.Printf("Error invalidating Redis balance: %v\n", err)
			}
			return nil
		}
		// Already paid for by the crashed attempt: resume sending.
	}

	// 2. Send to operator
	success := consumer.operator.SendSMS(sms.ToNumber, sms.Text)

	// 3. Finalize. From here on the operator call has happened, so we must not let
	// a shutdown abort the bookkeeping (that would cause a resend on redelivery).
	finalCtx := context.WithoutCancel(goContext)

	if success {
		err = consumer.retry(finalCtx, func(ctx context.Context) error {
			err := consumer.smsRepo.UpdateStatusFrom(ctx, sms.ID, domain.StatusPending, domain.StatusDelivered)
			if errors.Is(err, domain.ErrStatusNotChanged) {
				return nil // already finalized by an earlier attempt
			}
			return err
		})
		if err == nil {
			log.Printf("Processed SMS: %s, Status: %s\n", sms.ID, domain.StatusDelivered)
		}
		return err
	}

	// 4. Failed: mark FAILED and refund in ONE transaction, guarded by the
	// PENDING -> FAILED transition, so a crash can't leave a FAILED SMS without a
	// refund and a redelivery can't refund twice.
	refunded := false
	err = consumer.retry(finalCtx, func(ctx context.Context) error {
		refunded = false
		txErr := consumer.transactionManager.WithTransaction(ctx, func(transactionCtx context.Context) error {
			if err := consumer.smsRepo.UpdateStatusFrom(transactionCtx, sms.ID, domain.StatusPending, domain.StatusFailed); err != nil {
				return err
			}
			if err := consumer.userRepo.AddBalance(transactionCtx, sms.UserID, cost); err != nil {
				return err
			}
			return consumer.creditRepo.Create(transactionCtx, sms.UserID, cost, domain.CreditRefund)
		})
		if errors.Is(txErr, domain.ErrStatusNotChanged) {
			return nil // already finalized (and refunded, if needed) earlier
		}
		if txErr == nil {
			refunded = true
		}
		return txErr
	})
	if err != nil {
		return err
	}

	if refunded {
		if err := consumer.cache.AddBalance(finalCtx, sms.UserID, cost); err != nil {
			log.Printf("Error refunding Redis: %v\n", err)
			_ = consumer.cache.InvalidateBalance(finalCtx, sms.UserID)
		}
	}

	log.Printf("Processed SMS: %s, Status: %s\n", sms.ID, domain.StatusFailed)
	return nil
}

// retry runs fn until it succeeds. Each attempt gets its own timeout. It gives up
// only when goContext is cancelled (returns goContext's error).
func (consumer *Consumer) retry(goContext context.Context, fn func(ctx context.Context) error) error {
	for attempt := 1; ; attempt++ {
		attemptCtx, cancel := context.WithTimeout(context.WithoutCancel(goContext), 10*time.Second)
		err := fn(attemptCtx)
		cancel()
		if err == nil {
			return nil
		}
		log.Printf("Attempt %d failed (will retry): %v\n", attempt, err)

		backoff := consumer.retryBackoff * time.Duration(min(attempt, 10))
		if !sleepCtx(goContext, backoff) {
			return goContext.Err()
		}
	}
}

func sleepCtx(goContext context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-goContext.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (consumer *Consumer) Close() error {
	return consumer.reader.Close()
}
