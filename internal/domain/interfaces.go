package domain

import "context"

// Transaction Manager
type TransactionManager interface {
	WithTransaction(goContext context.Context, fn func(goContext context.Context) error) error
}

// Entity-specific Repositories
type UserRepository interface {
	GetBalance(goContext context.Context, userID int) (int, error)
	UpdateBalance(goContext context.Context, userID int, amount int) error
}

type SMSRepository interface {
	Create(goContext context.Context, sms *SMS) error
	UpdateStatus(goContext context.Context, id string, status string) error
	GetByID(goContext context.Context, id string) (*SMS, error)
	GetByUserID(goContext context.Context, userID int) ([]SMS, error)
}

type CreditRepository interface {
	Create(goContext context.Context, userID int, amount int, creditType string) error
}

// Cache interfaces
type CacheRepository interface {
	AddBalance(goContext context.Context, userID int, amount int) error
	DeductBalance(goContext context.Context, userID int, amount int) (int, error)
	SetBalance(goContext context.Context, userID int, balance int) error
}

// Message Queue interfaces
type MessageProducer interface {
	Produce(goContext context.Context, sms *SMS) error
}

// External Operator interfaces
type SMSOperator interface {
	SendSMS(toNumber string, text string) bool
}
