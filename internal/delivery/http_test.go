package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	balance   int
	getErr    error
	updateErr error
}

func (mock *mockUserRepository) GetBalance(goContext context.Context, userID int) (int, error) {
	return mock.balance, mock.getErr
}
func (mock *mockUserRepository) AddBalance(goContext context.Context, userID int, amount int) error {
	return mock.updateErr
}
func (mock *mockUserRepository) DeductBalance(goContext context.Context, userID int, amount int) error {
	return mock.updateErr
}

// Mock SMS Repo
type mockSMSRepository struct {
	mockData []domain.SMS
}

func (mock *mockSMSRepository) Create(goContext context.Context, sms *domain.SMS) error { return nil }
func (mock *mockSMSRepository) UpdateStatusFrom(goContext context.Context, id, from, to string) error {
	return nil
}
func (mock *mockSMSRepository) GetByID(goContext context.Context, id string) (*domain.SMS, error) {
	return nil, nil
}
func (mock *mockSMSRepository) GetByUserID(goContext context.Context, userID int) ([]domain.SMS, error) {
	return mock.mockData, nil
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
	addErr        error
	deductResults []int // consumed in order; the last value repeats
	deductCalls   int
	initCalls     int
	addCalls      int
	invalidations int
}

func (mock *mockCacheRepository) AddBalance(goContext context.Context, userID int, amount int) error {
	mock.addCalls++
	return mock.addErr
}
func (mock *mockCacheRepository) DeductBalance(goContext context.Context, userID int, amount int) (int, error) {
	index := min(mock.deductCalls, len(mock.deductResults)-1)
	mock.deductCalls++
	return mock.deductResults[index], nil
}
func (mock *mockCacheRepository) InitBalance(goContext context.Context, userID int, balance int) error {
	mock.initCalls++
	return nil
}
func (mock *mockCacheRepository) InvalidateBalance(goContext context.Context, userID int) error {
	mock.invalidations++
	return nil
}

// Mock Producer
type mockProducer struct {
	err error
}

func (mock *mockProducer) Produce(goContext context.Context, sms *domain.SMS) error { return mock.err }

type testDeps struct {
	userRepo *mockUserRepository
	smsRepo  *mockSMSRepository
	cache    *mockCacheRepository
	producer *mockProducer
}

func setupRouter() (*gin.Engine, *testDeps) {
	gin.SetMode(gin.TestMode)
	router := gin.New()

	deps := &testDeps{
		userRepo: &mockUserRepository{},
		smsRepo:  &mockSMSRepository{},
		cache:    &mockCacheRepository{deductResults: []int{1}},
		producer: &mockProducer{},
	}
	handler := NewHandler(
		&mockTransactionManager{},
		deps.userRepo,
		deps.smsRepo,
		&mockCreditRepository{},
		deps.cache,
		deps.producer,
	)
	RegisterRoutes(router, handler)
	return router, deps
}

func doJSON(router *gin.Engine, method, path string, payload any) *httptest.ResponseRecorder {
	body, _ := json.Marshal(payload)
	recorder := httptest.NewRecorder()
	request, _ := http.NewRequest(method, path, bytes.NewBuffer(body))
	request.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, request)
	return recorder
}

func validSMS() domain.SendSMSRequest {
	return domain.SendSMSRequest{UserID: 1, ToNumber: "09123456789", Text: "Test Message", IsExpress: true}
}

func TestHandler_TopUp_Success(testingT *testing.T) {
	router, _ := setupRouter()
	response := doJSON(router, "POST", "/api/v1/users/charge", domain.TopUpRequest{UserID: 1, Amount: 500})

	assert.Equal(testingT, http.StatusOK, response.Code)
	assert.Contains(testingT, response.Body.String(), "Top up successful")
}

func TestHandler_TopUp_InvalidAmount(testingT *testing.T) {
	router, _ := setupRouter()
	response := doJSON(router, "POST", "/api/v1/users/charge", domain.TopUpRequest{UserID: 1, Amount: -100})

	assert.Equal(testingT, http.StatusBadRequest, response.Code)
	assert.Contains(testingT, response.Body.String(), "Amount must be greater than zero")
}

func TestHandler_TopUp_UserNotFound(testingT *testing.T) {
	router, deps := setupRouter()
	deps.userRepo.updateErr = domain.ErrUserNotFound

	response := doJSON(router, "POST", "/api/v1/users/charge", domain.TopUpRequest{UserID: 99, Amount: 100})

	assert.Equal(testingT, http.StatusNotFound, response.Code)
}

func TestHandler_TopUp_CacheFailureInvalidatesKey(testingT *testing.T) {
	router, deps := setupRouter()
	deps.cache.addErr = errors.New("redis down")

	response := doJSON(router, "POST", "/api/v1/users/charge", domain.TopUpRequest{UserID: 1, Amount: 100})

	assert.Equal(testingT, http.StatusOK, response.Code)
	assert.Equal(testingT, 1, deps.cache.invalidations)
}

func TestHandler_SendSMS_Success(testingT *testing.T) {
	router, _ := setupRouter()
	response := doJSON(router, "POST", "/api/v1/sms/send", validSMS())

	assert.Equal(testingT, http.StatusOK, response.Code)
	assert.Contains(testingT, response.Body.String(), "SMS queued successfully")
}

func TestHandler_SendSMS_InsufficientBalance(testingT *testing.T) {
	router, deps := setupRouter()
	deps.cache.deductResults = []int{0}

	response := doJSON(router, "POST", "/api/v1/sms/send", validSMS())

	assert.Equal(testingT, http.StatusPaymentRequired, response.Code)
	assert.Contains(testingT, response.Body.String(), "Insufficient balance")
}

func TestHandler_SendSMS_CacheMissLoadsFromDB(testingT *testing.T) {
	router, deps := setupRouter()
	deps.cache.deductResults = []int{-1, 1}
	deps.userRepo.balance = 100

	response := doJSON(router, "POST", "/api/v1/sms/send", validSMS())

	assert.Equal(testingT, http.StatusOK, response.Code)
	assert.Equal(testingT, 1, deps.cache.initCalls)
	assert.Equal(testingT, 2, deps.cache.deductCalls)
}

func TestHandler_SendSMS_UnknownUser(testingT *testing.T) {
	router, deps := setupRouter()
	deps.cache.deductResults = []int{-1}
	deps.userRepo.getErr = domain.ErrUserNotFound

	response := doJSON(router, "POST", "/api/v1/sms/send", validSMS())

	assert.Equal(testingT, http.StatusNotFound, response.Code)
}

func TestHandler_SendSMS_ProduceFailureRefundsCache(testingT *testing.T) {
	router, deps := setupRouter()
	deps.producer.err = errors.New("kafka down")

	response := doJSON(router, "POST", "/api/v1/sms/send", validSMS())

	assert.Equal(testingT, http.StatusInternalServerError, response.Code)
	assert.Equal(testingT, 1, deps.cache.addCalls, "reserved credit must be given back")
}

func TestHandler_SendSMS_Validation(testingT *testing.T) {
	cases := map[string]domain.SendSMSRequest{
		"missing user":  {ToNumber: "09123456789", Text: "hi"},
		"missing text":  {UserID: 1, ToNumber: "09123456789"},
		"missing to":    {UserID: 1, Text: "hi"},
		"invalid phone": {UserID: 1, ToNumber: "not-a-number", Text: "hi"},
	}
	for name, request := range cases {
		testingT.Run(name, func(testingT *testing.T) {
			router, deps := setupRouter()
			response := doJSON(router, "POST", "/api/v1/sms/send", request)

			assert.Equal(testingT, http.StatusBadRequest, response.Code)
			assert.Equal(testingT, 0, deps.cache.deductCalls, "balance must not be touched")
		})
	}
}

func TestHandler_GetReports_Success(testingT *testing.T) {
	router, deps := setupRouter()
	deps.smsRepo.mockData = []domain.SMS{
		{ID: "uuid-1", UserID: 1, ToNumber: "09123456789", Status: "DELIVERED"},
	}

	recorder := httptest.NewRecorder()
	request, _ := http.NewRequest("GET", "/api/v1/users/1/report", nil)
	router.ServeHTTP(recorder, request)

	assert.Equal(testingT, http.StatusOK, recorder.Code)
	assert.Contains(testingT, recorder.Body.String(), "DELIVERED")
}

func TestHandler_GetReports_InvalidUser(testingT *testing.T) {
	router, _ := setupRouter()

	recorder := httptest.NewRecorder()
	request, _ := http.NewRequest("GET", "/api/v1/users/abc/report", nil)
	router.ServeHTTP(recorder, request)

	assert.Equal(testingT, http.StatusBadRequest, recorder.Code)
}
