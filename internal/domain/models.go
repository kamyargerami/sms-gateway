package domain

import "time"

type User struct {
	ID        int       `json:"id"`
	Balance   int       `json:"balance"`
	CreatedAt time.Time `json:"created_at"`
}

type SMS struct {
	ID        string    `json:"id"`
	UserID    int       `json:"user_id"`
	ToNumber  string    `json:"to_number"`
	Text      string    `json:"text"`
	Status    string    `json:"status"` // PENDING, DELIVERED, FAILED
	IsExpress bool      `json:"is_express"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type TopUpRequest struct {
	UserID int `json:"user_id"`
	Amount int `json:"amount"`
}

type SendSMSRequest struct {
	UserID    int    `json:"user_id"`
	ToNumber  string `json:"to_number"`
	Text      string `json:"text"`
	IsExpress bool   `json:"is_express"`
}
