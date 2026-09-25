package delivery

import (
	"context"
	"errors"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"sms/internal/config"
	"sms/internal/domain"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// phoneNumberPattern accepts an optional leading '+' followed by 8-15 digits (E.164-ish).
var phoneNumberPattern = regexp.MustCompile(`^\+?[0-9]{8,15}$`)

type Handler struct {
	transactionManager domain.TransactionManager
	userRepository     domain.UserRepository
	smsRepository      domain.SMSRepository
	creditRepository   domain.CreditRepository
	cache              domain.CacheRepository
	producer           domain.MessageProducer
}

func NewHandler(
	transactionManager domain.TransactionManager,
	userRepository domain.UserRepository,
	smsRepository domain.SMSRepository,
	creditRepository domain.CreditRepository,
	cache domain.CacheRepository,
	producer domain.MessageProducer,
) *Handler {
	return &Handler{
		transactionManager: transactionManager,
		userRepository:     userRepository,
		smsRepository:      smsRepository,
		creditRepository:   creditRepository,
		cache:              cache,
		producer:           producer,
	}
}

func (handler *Handler) TopUp(ginContext *gin.Context) {
	var request domain.TopUpRequest
	if err := ginContext.ShouldBindJSON(&request); err != nil {
		ginContext.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if request.Amount <= 0 {
		ginContext.JSON(http.StatusBadRequest, gin.H{"error": "Amount must be greater than zero"})
		return
	}

	goContext := ginContext.Request.Context()

	// Update DB atomically
	err := handler.transactionManager.WithTransaction(goContext, func(transactionContext context.Context) error {
		if err := handler.userRepository.AddBalance(transactionContext, request.UserID, request.Amount); err != nil {
			return err
		}
		return handler.creditRepository.Create(transactionContext, request.UserID, request.Amount, domain.CreditTopUp)
	})

	if err != nil {
		if errors.Is(err, domain.ErrUserNotFound) {
			ginContext.JSON(http.StatusNotFound, gin.H{"error": "User not found"})
			return
		}
		log.Printf("Top up failed for user %d: %v", request.UserID, err)
		ginContext.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to top up in DB"})
		return
	}

	// Update Redis (cache). If this fails, the cache would under-report the balance
	// forever (the key has no TTL), so drop it and let the next request reload it.
	if err := handler.cache.AddBalance(goContext, request.UserID, request.Amount); err != nil {
		log.Printf("Failed to sync balance to Redis: %v", err)
		if invalidateError := handler.cache.InvalidateBalance(goContext, request.UserID); invalidateError != nil {
			log.Printf("Failed to invalidate Redis balance for user %d: %v", request.UserID, invalidateError)
		}
	}

	ginContext.JSON(http.StatusOK, gin.H{"message": "Top up successful"})
}

func (handler *Handler) SendSMS(ginContext *gin.Context) {
	var request domain.SendSMSRequest
	if err := ginContext.ShouldBindJSON(&request); err != nil {
		ginContext.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if !phoneNumberPattern.MatchString(request.ToNumber) {
		ginContext.JSON(http.StatusBadRequest, gin.H{"error": "Invalid phone number"})
		return
	}

	cost := config.GetSMSCost()
	goContext := ginContext.Request.Context()

	result, err := handler.cache.DeductBalance(goContext, request.UserID, cost)
	if err != nil {
		ginContext.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to check balance"})
		return
	}

	if result == -1 {
		balance, err := handler.userRepository.GetBalance(goContext, request.UserID)
		if err != nil {
			if errors.Is(err, domain.ErrUserNotFound) {
				ginContext.JSON(http.StatusNotFound, gin.H{"error": "User not found"})
				return
			}
			ginContext.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load balance"})
			return
		}

		// SET NX: if a concurrent request already initialized the key, keep its value.
		if err := handler.cache.InitBalance(goContext, request.UserID, balance); err != nil {
			ginContext.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to sync cache"})
			return
		}

		result, err = handler.cache.DeductBalance(goContext, request.UserID, cost)
		if err != nil || result == -1 {
			ginContext.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to deduct balance after sync"})
			return
		}
	}

	if result == 0 {
		ginContext.JSON(http.StatusPaymentRequired, gin.H{"error": "Insufficient balance"})
		return
	}

	now := time.Now()
	sms := &domain.SMS{
		ID:        uuid.New().String(),
		UserID:    request.UserID,
		ToNumber:  request.ToNumber,
		Text:      request.Text,
		Status:    domain.StatusPending,
		IsExpress: request.IsExpress,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := handler.producer.Produce(goContext, sms); err != nil {
		log.Printf("Failed to publish to Kafka: %v", err)
		// The SMS was never queued, so give the reserved credit back. Use a context
		// that survives client disconnects, otherwise the refund could be skipped.
		refundContext, cancel := context.WithTimeout(context.WithoutCancel(goContext), 2*time.Second)
		defer cancel()
		if refundError := handler.cache.AddBalance(refundContext, request.UserID, cost); refundError != nil {
			log.Printf("Failed to refund Redis balance for user %d: %v", request.UserID, refundError)
			_ = handler.cache.InvalidateBalance(refundContext, request.UserID)
		}
		ginContext.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to queue SMS"})
		return
	}

	ginContext.JSON(http.StatusOK, gin.H{"message": "SMS queued successfully", "id": sms.ID})
}

func (handler *Handler) GetReports(ginContext *gin.Context) {
	userIDText := ginContext.Param("user_id")
	userID, err := strconv.Atoi(userIDText)
	if err != nil || userID <= 0 {
		ginContext.JSON(http.StatusBadRequest, gin.H{"error": "Invalid user ID"})
		return
	}

	reports, err := handler.smsRepository.GetByUserID(ginContext.Request.Context(), userID)
	if err != nil {
		ginContext.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch reports"})
		return
	}

	ginContext.JSON(http.StatusOK, gin.H{"reports": reports})
}

// RegisterRoutes wires the handlers. Shared by main and tests so the paths used
// in tests are the real ones.
func RegisterRoutes(router *gin.Engine, handler *Handler) {
	apiVersion1 := router.Group("/api/v1")
	{
		apiVersion1.POST("/users/charge", handler.TopUp)
		apiVersion1.POST("/sms/send", handler.SendSMS)
		apiVersion1.GET("/users/:user_id/report", handler.GetReports)
	}
}
