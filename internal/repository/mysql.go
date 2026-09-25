package repository

import (
	"database/sql"
	"fmt"
	"sms/internal/config"
	"sms/internal/domain"

	"github.com/go-sql-driver/mysql"
)

type MySQLRepository struct {
	db *sql.DB
}

func NewMySQLRepository(dsn string) (*MySQLRepository, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return &MySQLRepository{db: db}, nil
}

func (r *MySQLRepository) TopUpUser(userID int, amount int) error {
	tx, err := r.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Update user balance
	_, err = tx.Exec("UPDATE users SET balance = balance + ? WHERE id = ?", amount, userID)
	if err != nil {
		return err
	}

	// Record transaction
	_, err = tx.Exec("INSERT INTO transactions (user_id, amount, type) VALUES (?, ?, 'TOPUP')", userID, amount)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (r *MySQLRepository) RefundUser(userID int, amount int) error {
	tx, err := r.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Update user balance
	_, err = tx.Exec("UPDATE users SET balance = balance + ? WHERE id = ?", amount, userID)
	if err != nil {
		return err
	}

	// Record transaction
	_, err = tx.Exec("INSERT INTO transactions (user_id, amount, type) VALUES (?, ?, 'REFUND')", userID, amount)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (r *MySQLRepository) CreateSMS(sms *domain.SMS) error {
	tx, err := r.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Calculate cost
	cost := config.GetSMSCost()

	// 1. Update user balance in MySQL FIRST
	// This acquires an Exclusive (X) lock on the users row immediately,
	// preventing deadlocks caused by concurrent Foreign Key Shared (S) locks.
	_, err = tx.Exec("UPDATE users SET balance = balance - ? WHERE id = ?", cost, sms.UserID)
	if err != nil {
		return err
	}

	// 2. Insert SMS record
	_, err = tx.Exec(
		"INSERT INTO sms_records (id, user_id, to_number, text, status, is_express) VALUES (?, ?, ?, ?, ?, ?)",
		sms.ID, sms.UserID, sms.ToNumber, sms.Text, sms.Status, sms.IsExpress,
	)
	if err != nil {
		if mysqlErr, ok := err.(*mysql.MySQLError); ok && mysqlErr.Number == 1062 {
			return domain.ErrDuplicateRecord
		}
		return err
	}

	// 3. Record transaction
	_, err = tx.Exec("INSERT INTO transactions (user_id, amount, type) VALUES (?, ?, 'SMS_SENT')", sms.UserID, cost)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (r *MySQLRepository) UpdateSMSStatus(id string, status string) error {
	_, err := r.db.Exec("UPDATE sms_records SET status = ? WHERE id = ?", status, id)
	return err
}

func (r *MySQLRepository) GetUserSMS(userID int) ([]domain.SMS, error) {
	rows, err := r.db.Query("SELECT id, user_id, to_number, text, status, is_express, created_at, updated_at FROM sms_records WHERE user_id = ? ORDER BY created_at DESC LIMIT 100", userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var smsList []domain.SMS
	for rows.Next() {
		var s domain.SMS
		if err := rows.Scan(&s.ID, &s.UserID, &s.ToNumber, &s.Text, &s.Status, &s.IsExpress, &s.CreatedAt, &s.UpdatedAt); err != nil {
			return nil, err
		}
		smsList = append(smsList, s)
	}
	return smsList, nil
}

func (r *MySQLRepository) GetUserBalance(userID int) (int, error) {
	var balance int
	err := r.db.QueryRow("SELECT balance FROM users WHERE id = ?", userID).Scan(&balance)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("user not found")
		}
		return 0, err
	}
	return balance, nil
}
