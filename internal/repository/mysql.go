package repository

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sms/internal/domain"

	"github.com/go-sql-driver/mysql"
)

type transactionKey struct{}

// InjectTransaction puts the sql.Tx into context
func InjectTransaction(ctx context.Context, sqlTransaction *sql.Tx) context.Context {
	return context.WithValue(ctx, transactionKey{}, sqlTransaction)
}

// ExtractTransaction pulls the sql.Tx from context if it exists
func ExtractTransaction(ctx context.Context) *sql.Tx {
	if sqlTransaction, ok := ctx.Value(transactionKey{}).(*sql.Tx); ok {
		return sqlTransaction
	}
	return nil
}

// Queryer is an interface that matches both *sql.DB and *sql.Tx
type Queryer interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

func getQueryer(ctx context.Context, db *sql.DB) Queryer {
	if sqlTransaction := ExtractTransaction(ctx); sqlTransaction != nil {
		return sqlTransaction
	}
	return db
}

// TransactionManager Implementation
type MySQLTransactionManager struct {
	db *sql.DB
}

func NewMySQLTransactionManager(db *sql.DB) *MySQLTransactionManager {
	return &MySQLTransactionManager{db: db}
}

func (m *MySQLTransactionManager) WithTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	sqlTransaction, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	transactionCtx := InjectTransaction(ctx, sqlTransaction)

	if err := fn(transactionCtx); err != nil {
		if rbErr := sqlTransaction.Rollback(); rbErr != nil {
			log.Printf("Failed to rollback transaction: %v\n", rbErr)
		}
		return err
	}
	return sqlTransaction.Commit()
}

// User Repository
type MySQLUserRepository struct {
	db *sql.DB
}

func NewMySQLUserRepository(db *sql.DB) *MySQLUserRepository {
	return &MySQLUserRepository{db: db}
}

func (r *MySQLUserRepository) GetBalance(ctx context.Context, userID int) (int, error) {
	q := getQueryer(ctx, r.db)
	var balance int
	err := q.QueryRowContext(ctx, "SELECT balance FROM users WHERE id = ?", userID).Scan(&balance)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("user not found")
		}
		return 0, err
	}
	return balance, nil
}

func (r *MySQLUserRepository) UpdateBalance(ctx context.Context, userID int, amount int) error {
	q := getQueryer(ctx, r.db)
	_, err := q.ExecContext(ctx, "UPDATE users SET balance = balance + ? WHERE id = ?", amount, userID)
	return err
}

// SMS Repository
type MySQLSMSRepository struct {
	db *sql.DB
}

func NewMySQLSMSRepository(db *sql.DB) *MySQLSMSRepository {
	return &MySQLSMSRepository{db: db}
}

func (r *MySQLSMSRepository) Create(ctx context.Context, sms *domain.SMS) error {
	q := getQueryer(ctx, r.db)
	_, err := q.ExecContext(ctx,
		"INSERT INTO sms_records (id, user_id, to_number, text, status, is_express) VALUES (?, ?, ?, ?, ?, ?)",
		sms.ID, sms.UserID, sms.ToNumber, sms.Text, sms.Status, sms.IsExpress,
	)
	if err != nil {
		if mysqlErr, ok := err.(*mysql.MySQLError); ok && mysqlErr.Number == 1062 {
			return domain.ErrDuplicateRecord
		}
		return err
	}
	return nil
}

func (r *MySQLSMSRepository) UpdateStatus(ctx context.Context, id string, status string) error {
	q := getQueryer(ctx, r.db)
	_, err := q.ExecContext(ctx, "UPDATE sms_records SET status = ? WHERE id = ?", status, id)
	return err
}

func (r *MySQLSMSRepository) GetByID(ctx context.Context, id string) (*domain.SMS, error) {
	q := getQueryer(ctx, r.db)
	var s domain.SMS
	err := q.QueryRowContext(ctx, "SELECT id, user_id, to_number, text, status, is_express, created_at, updated_at FROM sms_records WHERE id = ?", id).
		Scan(&s.ID, &s.UserID, &s.ToNumber, &s.Text, &s.Status, &s.IsExpress, &s.CreatedAt, &s.UpdatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("sms not found")
		}
		return nil, err
	}
	return &s, nil
}

func (r *MySQLSMSRepository) GetByUserID(ctx context.Context, userID int) ([]domain.SMS, error) {
	q := getQueryer(ctx, r.db)
	rows, err := q.QueryContext(ctx, "SELECT id, user_id, to_number, text, status, is_express, created_at, updated_at FROM sms_records WHERE user_id = ? ORDER BY created_at DESC LIMIT 100", userID)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	var smsList []domain.SMS
	for rows.Next() {
		var s domain.SMS
		if err := rows.Scan(&s.ID, &s.UserID, &s.ToNumber, &s.Text, &s.Status, &s.IsExpress, &s.CreatedAt, &s.UpdatedAt); err != nil {
			return nil, err
		}
		smsList = append(smsList, s)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return smsList, nil
}

// Transaction Repository
type MySQLCreditRepository struct {
	db *sql.DB
}

func NewMySQLCreditRepository(db *sql.DB) *MySQLCreditRepository {
	return &MySQLCreditRepository{db: db}
}

func (r *MySQLCreditRepository) Create(ctx context.Context, userID int, amount int, creditType string) error {
	q := getQueryer(ctx, r.db)
	_, err := q.ExecContext(ctx, "INSERT INTO credits (user_id, amount, type) VALUES (?, ?, ?)", userID, amount, creditType)
	return err
}

// ConnectDB helper
func ConnectDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}
