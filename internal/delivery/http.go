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
		if err := handler.userRepo.AddBalance(transactionCtx, request.UserID, request.Amount); err != nil {
			return err
		}
		return handler.creditRepo.Create(transactionCtx, request.UserID, request.Amount, domain.CreditTopUp)
	})

	if err != nil {
		if errors.Is(err, domain.ErrUserNotFound) {
			ginCtx.JSON(http.StatusNotFound, gin.H{"error": "User not found"})
			return
		}
		log.Printf("Top up failed for user %d: %v", request.UserID, err)
		ginCtx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to top up in DB"})
		return
	}

	// Update Redis (cache). If this fails, the cache would under-report the balance
	// forever (the key has no TTL), so drop it and let the next request reload it.
	if err := handler.cache.AddBalance(goContext, request.UserID, request.Amount); err != nil {
		log.Printf("Failed to sync balance to Redis: %v", err)
		if invErr := handler.cache.InvalidateBalance(goContext, request.UserID); invErr != nil {
			log.Printf("Failed to invalidate Redis balance for user %d: %v", request.UserID, invErr)
		}
	}

	ginCtx.JSON(http.StatusOK, gin.H{"message": "Top up successful"})
}

func (handler *Handler) SendSMS(ginCtx *gin.Context) {
	var request domain.SendSMSRequest
	if err := ginCtx.ShouldBindJSON(&request); err != nil {
		ginCtx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if !phoneNumberPattern.MatchString(request.ToNumber) {
		ginCtx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid phone number"})
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
			if errors.Is(err, domain.ErrUserNotFound) {
				ginCtx.JSON(http.StatusNotFound, gin.H{"error": "User not found"})
				return
			}
			ginCtx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load balance"})
			return
		}

		// SET NX: if a concurrent request already initialized the key, keep its value.
		if err := handler.cache.InitBalance(goContext, request.UserID, balance); err != nil {
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
		refundCtx, cancel := context.WithTimeout(context.WithoutCancel(goContext), 2*time.Second)
		defer cancel()
		if refundErr := handler.cache.AddBalance(refundCtx, request.UserID, cost); refundErr != nil {
			log.Printf("Failed to refund Redis balance for user %d: %v", request.UserID, refundErr)
			_ = handler.cache.InvalidateBalance(refundCtx, request.UserID)
		}
		ginCtx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to queue SMS"})
		return
	}

	ginCtx.JSON(http.StatusOK, gin.H{"message": "SMS queued successfully", "id": sms.ID})
}

func (handler *Handler) GetReports(ginCtx *gin.Context) {
	userIDStr := ginCtx.Param("user_id")
	userID, err := strconv.Atoi(userIDStr)
	if err != nil || userID <= 0 {
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

// RegisterRoutes wires the handlers. Shared by main and tests so the paths used
// in tests are the real ones.
func RegisterRoutes(router *gin.Engine, handler *Handler) {
	v1 := router.Group("/api/v1")
	{
		v1.POST("/users/charge", handler.TopUp)
		v1.POST("/sms/send", handler.SendSMS)
		v1.GET("/users/:user_id/report", handler.GetReports)
	}
}
