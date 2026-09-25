package delivery

import (
	"context"
	"log"
	"net/http"
	"strconv"
	"time"

	"sms/internal/config"
	"sms/internal/domain"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type Handler struct {
	transactionManager domain.TransactionManager
	userRepo           domain.UserRepository
	smsRepo            domain.SMSRepository
	creditRepo         domain.CreditRepository
	cache              domain.CacheRepository
	producer           domain.MessageProducer
}

func NewHandler(
	transactionManager domain.TransactionManager,
	userRepo domain.UserRepository,
	smsRepo domain.SMSRepository,
	creditRepo domain.CreditRepository,
	cache domain.CacheRepository,
	producer domain.MessageProducer,
) *Handler {
	return &Handler{
		transactionManager: transactionManager,
		userRepo:           userRepo,
		smsRepo:            smsRepo,
		creditRepo:         creditRepo,
		cache:              cache,
		producer:           producer,
	}
}

func (handler *Handler) TopUp(ginCtx *gin.Context) {
	var request domain.TopUpRequest
	if err := ginCtx.ShouldBindJSON(&request); err != nil {
		ginCtx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if request.Amount <= 0 {
		ginCtx.JSON(http.StatusBadRequest, gin.H{"error": "Amount must be greater than zero"})
		return
	}

	goContext := ginCtx.Request.Context()

	// Update DB atomically
	err := handler.transactionManager.WithTransaction(goContext, func(transactionCtx context.Context) error {
		if err := handler.userRepo.UpdateBalance(transactionCtx, request.UserID, request.Amount); err != nil {
			return err
		}
		return handler.creditRepo.Create(transactionCtx, request.UserID, request.Amount, "TOPUP")
	})

	if err != nil {
		ginCtx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to top up in DB"})
		return
	}

	// Update Redis (cache)
	err = handler.cache.AddBalance(goContext, request.UserID, request.Amount)
	if err != nil {
		log.Printf("Failed to sync balance to Redis: %v", err)
	}

	ginCtx.JSON(http.StatusOK, gin.H{"message": "Top up successful"})
}

func (handler *Handler) SendSMS(ginCtx *gin.Context) {
	var request domain.SendSMSRequest
	if err := ginCtx.ShouldBindJSON(&request); err != nil {
		ginCtx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	cost := config.GetSMSCost()
	goContext := ginCtx.Request.Context()

	result, err := handler.cache.DeductBalance(goContext, request.UserID, cost)
	if err != nil {
		ginCtx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to check balance"})
		return
	}

	if result == -1 {
		balance, err := handler.userRepo.GetBalance(goContext, request.UserID)
		if err != nil {
			ginCtx.JSON(http.StatusNotFound, gin.H{"error": "User not found or db error"})
			return
		}

		if err := handler.cache.SetBalance(goContext, request.UserID, balance); err != nil {
			ginCtx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to sync cache"})
			return
		}

		result, err = handler.cache.DeductBalance(goContext, request.UserID, cost)
		if err != nil || result == -1 {
			ginCtx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to deduct balance after sync"})
			return
		}
	}

	if result == 0 {
		ginCtx.JSON(http.StatusPaymentRequired, gin.H{"error": "Insufficient balance"})
		return
	}

	sms := &domain.SMS{
		ID:        uuid.New().String(),
		UserID:    request.UserID,
		ToNumber:  request.ToNumber,
		Text:      request.Text,
		Status:    "PENDING",
		IsExpress: request.IsExpress,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	if err := handler.producer.Produce(goContext, sms); err != nil {
		log.Printf("Failed to publish to Kafka: %v", err)
		ginCtx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to queue SMS"})
		return
	}

	ginCtx.JSON(http.StatusOK, gin.H{"message": "SMS queued successfully", "id": sms.ID})
}

func (handler *Handler) GetReports(ginCtx *gin.Context) {
	userIDStr := ginCtx.Param("user_id")
	userID, err := strconv.Atoi(userIDStr)
	if err != nil {
		ginCtx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid user ID"})
		return
	}

	reports, err := handler.smsRepo.GetByUserID(ginCtx.Request.Context(), userID)
	if err != nil {
		ginCtx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch reports"})
		return
	}

	ginCtx.JSON(http.StatusOK, gin.H{"reports": reports})
}
