package domain

import "time"

const (
	StatusPending   = "PENDING"
	StatusDelivered = "DELIVERED"
	StatusFailed    = "FAILED"
	StatusExpired   = "EXPIRED"

	CreditTopUp   = "TOPUP"
	CreditSMSSent = "SMS_SENT"
	CreditRefund  = "REFUND"
)

type User struct {
	ID        int       `json:"id"`
	Balance   int       `json:"balance"`
	CreatedAt time.Time `json:"created_at"`
}

type SMS struct {
	ID        string     `json:"id"`
	UserID    int        `json:"user_id"`
	ToNumber  string     `json:"to_number"`
	Text      string     `json:"text"`
	Status    string     `json:"status"` // PENDING, DELIVERED, FAILED, EXPIRED
	IsExpress bool       `json:"is_express"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
}

// IsExpiredAt reports whether the delivery deadline has passed at instant now.
func (sms *SMS) IsExpiredAt(now time.Time) bool {
	return sms.ExpiresAt != nil && !now.Before(*sms.ExpiresAt)
}

type TopUpRequest struct {
	UserID int `json:"user_id" binding:"required,gt=0"`
	Amount int `json:"amount"`
}

type SendSMSRequest struct {
	UserID    int    `json:"user_id" binding:"required,gt=0"`
	ToNumber  string `json:"to_number" binding:"required,max=20"`
	Text      string `json:"text" binding:"required"`
	IsExpress bool   `json:"is_express"`
}
