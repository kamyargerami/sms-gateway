package repository

import (
	"context"
	"database/sql"
	"errors"
	"sms/internal/domain"
)

// User Repository
type MySQLUserRepository struct {
	database *sql.DB
}

func NewMySQLUserRepository(database *sql.DB) *MySQLUserRepository {
	return &MySQLUserRepository{database: database}
}

func (repository *MySQLUserRepository) GetBalance(goContext context.Context, userID int) (int, error) {
	queryer := getQueryer(goContext, repository.database)
	var balance int
	err := queryer.QueryRowContext(goContext, "SELECT balance FROM users WHERE id = ?", userID).Scan(&balance)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, domain.ErrUserNotFound
		}
		return 0, err
	}
	return balance, nil
}

func (repository *MySQLUserRepository) AddBalance(goContext context.Context, userID int, amount int) error {
	queryer := getQueryer(goContext, repository.database)
	result, err := queryer.ExecContext(goContext, "UPDATE users SET balance = balance + ? WHERE id = ?", amount, userID)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return domain.ErrUserNotFound
	}
	return nil
}

// DeductBalance is the source-of-truth guard: the WHERE clause makes the
// check-and-decrement atomic inside InnoDB, so the balance can never go below zero
// even if the Redis cache is stale (e.g. after a Redis restart/eviction).
func (repository *MySQLUserRepository) DeductBalance(goContext context.Context, userID int, amount int) error {
	queryer := getQueryer(goContext, repository.database)
	result, err := queryer.ExecContext(goContext,
		"UPDATE users SET balance = balance - ? WHERE id = ? AND balance >= ?", amount, userID, amount)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		if _, err := repository.GetBalance(goContext, userID); err != nil {
			return err // ErrUserNotFound or a DB error
		}
		return domain.ErrInsufficientBalance
	}
	return nil
}
