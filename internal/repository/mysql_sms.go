package repository

import (
	"context"
	"database/sql"
	"errors"
	"sms/internal/domain"

	"github.com/go-sql-driver/mysql"
)

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
		"INSERT INTO sms_records (id, user_id, to_number, text, status, is_express, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
		sms.ID, sms.UserID, sms.ToNumber, sms.Text, sms.Status, sms.IsExpress, sms.ExpiresAt,
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
	row := queryer.QueryRowContext(goContext, "SELECT "+smsColumns+" FROM sms_records WHERE id = ?", id)
	smsRecord, err := scanSMS(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.ErrSMSNotFound
		}
		return nil, err
	}
	return smsRecord, nil
}

func (repository *MySQLSMSRepository) GetByUserID(goContext context.Context, userID int, limit int, offset int) ([]domain.SMS, error) {
	queryer := getQueryer(goContext, repository.database)
	rows, err := queryer.QueryContext(goContext, "SELECT "+smsColumns+" FROM sms_records WHERE user_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?", userID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	smsList := make([]domain.SMS, 0)
	for rows.Next() {
		smsRecord, err := scanSMS(rows)
		if err != nil {
			return nil, err
		}
		smsList = append(smsList, *smsRecord)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return smsList, nil
}

const smsColumns = "id, user_id, to_number, text, status, is_express, expires_at, created_at, updated_at"

type rowScanner interface {
	Scan(destinations ...any) error
}

func scanSMS(row rowScanner) (*domain.SMS, error) {
	var smsRecord domain.SMS
	var expiresAt sql.NullTime
	if err := row.Scan(&smsRecord.ID, &smsRecord.UserID, &smsRecord.ToNumber, &smsRecord.Text, &smsRecord.Status,
		&smsRecord.IsExpress, &expiresAt, &smsRecord.CreatedAt, &smsRecord.UpdatedAt); err != nil {
		return nil, err
	}
	if expiresAt.Valid {
		smsRecord.ExpiresAt = &expiresAt.Time
	}
	return &smsRecord, nil
}
