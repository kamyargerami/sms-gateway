package repository

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"sms/internal/config"
	"sms/internal/domain"
	"time"

	"github.com/go-sql-driver/mysql"
)

type transactionKey struct{}

// InjectTransaction puts the sql.Tx into context
func InjectTransaction(goContext context.Context, sqlTransaction *sql.Tx) context.Context {
	return context.WithValue(goContext, transactionKey{}, sqlTransaction)
}

// ExtractTransaction pulls the sql.Tx from context if it exists
func ExtractTransaction(goContext context.Context) *sql.Tx {
	if sqlTransaction, ok := goContext.Value(transactionKey{}).(*sql.Tx); ok {
		return sqlTransaction
	}
	return nil
}

// Queryer is an interface that matches both *sql.DB and *sql.Tx
type Queryer interface {
	ExecContext(goContext context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(goContext context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(goContext context.Context, query string, args ...interface{}) *sql.Row
}

func getQueryer(goContext context.Context, database *sql.DB) Queryer {
	if sqlTransaction := ExtractTransaction(goContext); sqlTransaction != nil {
		return sqlTransaction
	}
	return database
}

// TransactionManager Implementation
type MySQLTransactionManager struct {
	database *sql.DB
}

func NewMySQLTransactionManager(database *sql.DB) *MySQLTransactionManager {
	return &MySQLTransactionManager{database: database}
}

func (manager *MySQLTransactionManager) WithTransaction(goContext context.Context, operation func(goContext context.Context) error) error {
	sqlTransaction, err := manager.database.BeginTx(goContext, nil)
	if err != nil {
		return err
	}

	transactionContext := InjectTransaction(goContext, sqlTransaction)

	defer func() {
		// Guarantees the transaction is released even if operation panics.
		if recovered := recover(); recovered != nil {
			_ = sqlTransaction.Rollback()
			panic(recovered)
		}
	}()

	if err := operation(transactionContext); err != nil {
		if rollbackError := sqlTransaction.Rollback(); rollbackError != nil {
			log.Printf("Failed to rollback transaction: %v\n", rollbackError)
		}
		return err
	}
	return sqlTransaction.Commit()
}

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

// SMS Repository
type MySQLSMSRepository struct {
	database *sql.DB
}

func NewMySQLSMSRepository(database *sql.DB) *MySQLSMSRepository {
	return &MySQLSMSRepository{database: database}
}

func (repository *MySQLSMSRepository) Create(goContext context.Context, sms *domain.SMS) error {
	queryer := getQueryer(goContext, repository.database)
	_, err := queryer.ExecContext(goContext,
		"INSERT INTO sms_records (id, user_id, to_number, text, status, is_express) VALUES (?, ?, ?, ?, ?, ?)",
		sms.ID, sms.UserID, sms.ToNumber, sms.Text, sms.Status, sms.IsExpress,
	)
	if err != nil {
		var mysqlError *mysql.MySQLError
		if errors.As(err, &mysqlError) && mysqlError.Number == 1062 {
			return domain.ErrDuplicateRecord
		}
		return err
	}
	return nil
}

func (repository *MySQLSMSRepository) UpdateStatusFrom(goContext context.Context, id string, from string, to string) error {
	queryer := getQueryer(goContext, repository.database)
	result, err := queryer.ExecContext(goContext,
		"UPDATE sms_records SET status = ? WHERE id = ? AND status = ?", to, id, from)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return domain.ErrStatusNotChanged
	}
	return nil
}

func (repository *MySQLSMSRepository) GetByID(goContext context.Context, id string) (*domain.SMS, error) {
	queryer := getQueryer(goContext, repository.database)
	var smsRecord domain.SMS
	err := queryer.QueryRowContext(goContext, "SELECT id, user_id, to_number, text, status, is_express, created_at, updated_at FROM sms_records WHERE id = ?", id).
		Scan(&smsRecord.ID, &smsRecord.UserID, &smsRecord.ToNumber, &smsRecord.Text, &smsRecord.Status, &smsRecord.IsExpress, &smsRecord.CreatedAt, &smsRecord.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.ErrSMSNotFound
		}
		return nil, err
	}
	return &smsRecord, nil
}

func (repository *MySQLSMSRepository) GetByUserID(goContext context.Context, userID int) ([]domain.SMS, error) {
	queryer := getQueryer(goContext, repository.database)
	rows, err := queryer.QueryContext(goContext, "SELECT id, user_id, to_number, text, status, is_express, created_at, updated_at FROM sms_records WHERE user_id = ? ORDER BY created_at DESC LIMIT 100", userID)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	smsList := make([]domain.SMS, 0)
	for rows.Next() {
		var smsRecord domain.SMS
		if err := rows.Scan(&smsRecord.ID, &smsRecord.UserID, &smsRecord.ToNumber, &smsRecord.Text, &smsRecord.Status, &smsRecord.IsExpress, &smsRecord.CreatedAt, &smsRecord.UpdatedAt); err != nil {
			return nil, err
		}
		smsList = append(smsList, smsRecord)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return smsList, nil
}

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

// ConnectDatabase helper
func ConnectDatabase(dataSourceName string, pool config.DatabasePoolConfig) (*sql.DB, error) {
	database, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		return nil, err
	}
	// Without limits, database/sql opens unbounded connections under load and
	// quickly exhausts MySQL's max_connections.
	database.SetMaxOpenConns(pool.MaxOpenConnections)
	database.SetMaxIdleConns(pool.MaxIdleConnections)
	database.SetConnMaxLifetime(pool.ConnectionMaxLifetime)

	// MySQL's healthcheck can pass while the entrypoint is still running its
	// temporary init server, so retry for a while instead of crashing on boot.
	var pingError error
	for attempt := 1; attempt <= 30; attempt++ {
		if pingError = database.Ping(); pingError == nil {
			return database, nil
		}
		log.Printf("MySQL not ready (attempt %d/30): %v", attempt, pingError)
		time.Sleep(2 * time.Second)
	}
	_ = database.Close()
	return nil, pingError
}
