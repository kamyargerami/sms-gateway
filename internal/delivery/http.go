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

func (h *Handler) TopUp(c *gin.Context) {
	var req domain.TopUpRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.Amount <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Amount must be greater than zero"})
		return
	}

	ctx := c.Request.Context()

	// Update DB atomically
	err := h.transactionManager.WithTransaction(ctx, func(transactionCtx context.Context) error {
		if err := h.userRepo.UpdateBalance(transactionCtx, req.UserID, req.Amount); err != nil {
			return err
		}
		return h.creditRepo.Create(transactionCtx, req.UserID, req.Amount, "TOPUP")
	})

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to top up in DB"})
		return
	}

	// Update Redis (cache)
	err = h.cache.AddBalance(ctx, req.UserID, req.Amount)
	if err != nil {
		log.Printf("Failed to sync balance to Redis: %v", err)
	}

	c.JSON(http.StatusOK, gin.H{"message": "Top up successful"})
}

func (h *Handler) SendSMS(c *gin.Context) {
	var req domain.SendSMSRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	cost := config.GetSMSCost()
	ctx := c.Request.Context()

	res, err := h.cache.DeductBalance(ctx, req.UserID, cost)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to check balance"})
		return
	}

	if res == -1 {
		balance, err := h.userRepo.GetBalance(ctx, req.UserID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "User not found or db error"})
			return
		}

		if err := h.cache.SetBalance(ctx, req.UserID, balance); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to sync cache"})
			return
		}

		res, err = h.cache.DeductBalance(ctx, req.UserID, cost)
		if err != nil || res == -1 {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to deduct balance after sync"})
			return
		}
	}

	if res == 0 {
		c.JSON(http.StatusPaymentRequired, gin.H{"error": "Insufficient balance"})
		return
	}

	sms := &domain.SMS{
		ID:        uuid.New().String(),
		UserID:    req.UserID,
		ToNumber:  req.ToNumber,
		Text:      req.Text,
		Status:    "PENDING",
		IsExpress: req.IsExpress,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	if err := h.producer.Produce(ctx, sms); err != nil {
		log.Printf("Failed to publish to Kafka: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to queue SMS"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "SMS queued successfully", "id": sms.ID})
}

func (h *Handler) GetReports(c *gin.Context) {
	userIDStr := c.Param("user_id")
	userID, err := strconv.Atoi(userIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid user ID"})
		return
	}

	reports, err := h.smsRepo.GetByUserID(c.Request.Context(), userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch reports"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"reports": reports})
}
