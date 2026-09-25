package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sms/internal/domain"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

// Mock Transaction Manager
type mockTransactionManager struct{}

func (mock *mockTransactionManager) WithTransaction(goContext context.Context, fn func(goContext context.Context) error) error {
	return fn(goContext)
}

// Mock User Repo
type mockUserRepository struct {
	updateErr error
}

func (mock *mockUserRepository) GetBalance(goContext context.Context, userID int) (int, error) {
	return 0, nil
}
func (mock *mockUserRepository) UpdateBalance(goContext context.Context, userID int, amount int) error {
	return mock.updateErr
}

// Mock SMS Repo
type mockSMSRepository struct{}

func (mock *mockSMSRepository) Create(goContext context.Context, sms *domain.SMS) error { return nil }
func (mock *mockSMSRepository) UpdateStatus(goContext context.Context, id string, status string) error {
	return nil
}
func (mock *mockSMSRepository) GetByID(goContext context.Context, id string) (*domain.SMS, error) {
	return nil, nil
}
func (mock *mockSMSRepository) GetByUserID(goContext context.Context, userID int) ([]domain.SMS, error) {
	return nil, nil
}

// Mock Credit Repo
type mockCreditRepository struct {
	createErr error
}

func (mock *mockCreditRepository) Create(goContext context.Context, userID int, amount int, creditType string) error {
	return mock.createErr
}

// Mock Cache Repo
type mockCacheRepository struct {
	addErr error
}

func (mock *mockCacheRepository) AddBalance(goContext context.Context, userID int, amount int) error {
	return mock.addErr
}
func (mock *mockCacheRepository) DeductBalance(goContext context.Context, userID int, amount int) (int, error) {
	return 0, nil
}
func (mock *mockCacheRepository) SetBalance(goContext context.Context, userID int, balance int) error {
	return nil
}

// Mock Producer
type mockProducer struct{}

func (mock *mockProducer) Produce(goContext context.Context, sms *domain.SMS) error { return nil }

func setupRouter() (*gin.Engine, *Handler) {
	gin.SetMode(gin.TestMode)
	router := gin.Default()

	handler := NewHandler(
		&mockTransactionManager{},
		&mockUserRepository{},
		&mockSMSRepository{},
		&mockCreditRepository{},
		&mockCacheRepository{},
		&mockProducer{},
	)

	router.POST("/api/v1/topup", handler.TopUp)
	return router, handler
}

func TestHandler_TopUp_Success(testingT *testing.T) {
	router, _ := setupRouter()

	requestBody := domain.TopUpRequest{
		UserID: 1,
		Amount: 500,
	}
	body, _ := json.Marshal(requestBody)

	responseRecorder := httptest.NewRecorder()
	httpRequest, _ := http.NewRequest("POST", "/api/v1/topup", bytes.NewBuffer(body))
	httpRequest.Header.Set("Content-Type", "application/json")

	router.ServeHTTP(responseRecorder, httpRequest)

	assert.Equal(testingT, http.StatusOK, responseRecorder.Code)
	assert.Contains(testingT, responseRecorder.Body.String(), "Top up successful")
}

func TestHandler_TopUp_InvalidAmount(testingT *testing.T) {
	router, _ := setupRouter()

	requestBody := domain.TopUpRequest{
		UserID: 1,
		Amount: -100, // Invalid
	}
	body, _ := json.Marshal(requestBody)

	responseRecorder := httptest.NewRecorder()
	httpRequest, _ := http.NewRequest("POST", "/api/v1/topup", bytes.NewBuffer(body))
	httpRequest.Header.Set("Content-Type", "application/json")

	router.ServeHTTP(responseRecorder, httpRequest)

	assert.Equal(testingT, http.StatusBadRequest, responseRecorder.Code)
	assert.Contains(testingT, responseRecorder.Body.String(), "Amount must be greater than zero")
}
