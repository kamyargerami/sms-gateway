package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"sms/internal/config"
	"sms/internal/delivery"
	"sms/internal/kafka"
	"sms/internal/repository"

	"github.com/gin-gonic/gin"
)

func main() {
	databaseDSN := os.Getenv("DB_DSN")
	redisAddr := os.Getenv("REDIS_ADDR")
	kafkaBrokers := config.GetKafkaBrokers()
	port := os.Getenv("SERVER_PORT")

	if port == "" {
		port = "8080"
	}

	db, err := repository.ConnectDB(databaseDSN, config.GetDBPoolConfig())
	if err != nil {
		log.Fatalf("Failed to connect to MySQL: %v", err)
	}
	defer func() { _ = db.Close() }()

	transactionManager := repository.NewMySQLTransactionManager(db)
	userRepo := repository.NewMySQLUserRepository(db)
	smsRepo := repository.NewMySQLSMSRepository(db)
	creditRepo := repository.NewMySQLCreditRepository(db)

	redisRepo := repository.NewRedisRepository(redisAddr)
	producer := kafka.NewProducer(kafkaBrokers)
	defer producer.Close()

	handler := delivery.NewHandler(transactionManager, userRepo, smsRepo, creditRepo, redisRepo, producer)

	router := gin.Default()
	delivery.RegisterRoutes(router, handler)

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("Starting API server on port %s", port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Graceful shutdown: finish in-flight requests and flush the Kafka writers
	// (the deferred producer.Close) instead of dropping them on SIGTERM.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down API server...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}
}
