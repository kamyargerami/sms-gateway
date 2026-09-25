package domain

import "context"

// DatabaseRepository Database interfaces
type DatabaseRepository interface {
	TopUpUser(userID int, amount int) error
	RefundUser(userID int, amount int) error
	CreateSMS(sms *SMS) error
	UpdateSMSStatus(id string, status string) error
	GetUserSMS(userID int) ([]SMS, error)
	GetUserBalance(userID int) (int, error)
}

// CacheRepository Cache interfaces
type CacheRepository interface {
	AddBalance(ctx context.Context, userID int, amount int) error
	DeductBalance(ctx context.Context, userID int, amount int) (int, error)
	SetBalance(ctx context.Context, userID int, balance int) error
}

// MessageProducer Message Queue interfaces
type MessageProducer interface {
	Produce(ctx context.Context, sms *SMS) error
}

// SMSOperator External Operator interfaces
type SMSOperator interface {
	SendSMS(toNumber string, text string) bool
}
