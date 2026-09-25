package domain

import "context"

// Transaction Manager
type TransactionManager interface {
	WithTransaction(goContext context.Context, operation func(goContext context.Context) error) error
}

// Entity-specific Repositories
type UserRepository interface {
	GetBalance(goContext context.Context, userID int) (int, error)
	// AddBalance increases the balance. Returns ErrUserNotFound if the user does not exist.
	AddBalance(goContext context.Context, userID int, amount int) error
	// DeductBalance atomically decreases the balance only if it stays >= 0.
	// Returns ErrInsufficientBalance otherwise (or ErrUserNotFound).
	DeductBalance(goContext context.Context, userID int, amount int) error
}

type SMSRepository interface {
	Create(goContext context.Context, sms *SMS) error
	// UpdateStatusFrom performs a conditional transition from -> to.
	// Returns ErrStatusNotChanged if the record is not in the `from` state.
	UpdateStatusFrom(goContext context.Context, id string, from string, to string) error
	GetByID(goContext context.Context, id string) (*SMS, error)
	GetByUserID(goContext context.Context, userID int) ([]SMS, error)
}

type CreditRepository interface {
	Create(goContext context.Context, userID int, amount int, creditType string) error
}

// Cache interfaces
type CacheRepository interface {
	// AddBalance increments the cached balance only if the key already exists.
	AddBalance(goContext context.Context, userID int, amount int) error
	// DeductBalance returns 1 on success, 0 on insufficient balance, -1 on cache miss.
	DeductBalance(goContext context.Context, userID int, amount int) (int, error)
	// InitBalance sets the cached balance only if it is not already present (SET NX),
	// so concurrent cache-miss handlers cannot overwrite each other's deductions.
	InitBalance(goContext context.Context, userID int, balance int) error
	// InvalidateBalance drops the cached balance so the next request reloads it.
	InvalidateBalance(goContext context.Context, userID int) error
}

// Message Queue interfaces
type MessageProducer interface {
	Produce(goContext context.Context, sms *SMS) error
}

// External Operator interfaces
type SMSOperator interface {
	SendSMS(toNumber string, text string) bool
}
