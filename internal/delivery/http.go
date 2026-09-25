package delivery

import (
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
	dbRepo   domain.DatabaseRepository
	cache    domain.CacheRepository
	producer domain.MessageProducer
}

func NewHandler(dbRepo domain.DatabaseRepository, cache domain.CacheRepository, producer domain.MessageProducer) *Handler {
	return &Handler{
		dbRepo:   dbRepo,
		cache:    cache,
		producer: producer,
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

	// Update DB
	err := h.dbRepo.TopUpUser(req.UserID, req.Amount)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to top up in DB"})
		return
	}

	// Update Redis (cache). If it doesn't exist, this will create it or next read will fetch.
	err = h.cache.AddBalance(c.Request.Context(), req.UserID, req.Amount)
	if err != nil {
		log.Printf("Failed to sync balance to Redis: %v", err)
		// We still return success since DB is source of truth, but log the error
	}

	c.JSON(http.StatusOK, gin.H{"message": "Top up successful"})
}

func (h *Handler) SendSMS(c *gin.Context) {
	var req domain.SendSMSRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Cost of 1 SMS
	cost := config.GetSMSCost()

	ctx := c.Request.Context()

	// Use Lua script to atomically check and deduct balance
	res, err := h.cache.DeductBalance(ctx, req.UserID, cost)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to check balance"})
		return
	}

	if res == -1 {
		// Key didn't exist in Redis. Fetch from MySQL.
		balance, err := h.dbRepo.GetUserBalance(req.UserID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "User not found or db error"})
			return
		}

		// Set in Redis
		if err := h.cache.SetBalance(ctx, req.UserID, balance); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to sync cache"})
			return
		}

		// Retry deduction
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

	// Balance successfully deducted. Create SMS record.
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

	// Removed MySQL synchronous insert to improve throughput.
	// Worker will insert the record into MySQL asynchronously.

	// Publish to Kafka
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

	reports, err := h.dbRepo.GetUserSMS(userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch reports"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"reports": reports})
}
