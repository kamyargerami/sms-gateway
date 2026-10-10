package domain

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
