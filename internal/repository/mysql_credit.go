package repository

import (
	"context"
	"database/sql"
)

// Transaction Repository
type MySQLCreditRepository struct {
	database *sql.DB
}

func NewMySQLCreditRepository(database *sql.DB) *MySQLCreditRepository {
	return &MySQLCreditRepository{database: database}
}

func (repository *MySQLCreditRepository) Create(goContext context.Context, userID int, amount int, creditType string) error {
	queryer := getQueryer(goContext, repository.database)
	_, err := queryer.ExecContext(goContext, "INSERT INTO credits (user_id, amount, type) VALUES (?, ?, ?)", userID, amount, creditType)
	return err
}
