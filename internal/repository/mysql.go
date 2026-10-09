package repository

import (
	"context"
	"database/sql"
	"log"
	"sms/internal/config"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

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
