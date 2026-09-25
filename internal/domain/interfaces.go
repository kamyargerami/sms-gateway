package domain

import "context"

// Transaction Manager (Unit of Work)
type TransactionManager interface {
	WithTransaction(ctx context.Context, fn func(ctx context.Context) error) error
}

// Entity-specific Repositories
type UserRepository interface {
	GetBalance(ctx context.Context, userID int) (int, error)
	UpdateBalance(ctx context.Context, userID int, amount int) error
}

type SMSRepository interface {
	Create(ctx context.Context, sms *SMS) error
	UpdateStatus(ctx context.Context, id string, status string) error
	GetByUserID(ctx context.Context, userID int) ([]SMS, error)
}

type TransactionRepository interface {
	Create(ctx context.Context, userID int, amount int, transactionType string) error
}

// Cache interfaces
type CacheRepository interface {
	AddBalance(ctx context.Context, userID int, amount int) error
	DeductBalance(ctx context.Context, userID int, amount int) (int, error)
	SetBalance(ctx context.Context, userID int, balance int) error
}

// Message Queue interfaces
type MessageProducer interface {
	Produce(ctx context.Context, sms *SMS) error
}

// External Operator interfaces
type SMSOperator interface {
	SendSMS(toNumber string, text string) bool
}
