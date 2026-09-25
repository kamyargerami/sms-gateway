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
	redisAddress := os.Getenv("REDIS_ADDR")
	kafkaBrokers := config.GetKafkaBrokers()
	port := os.Getenv("SERVER_PORT")

	if port == "" {
		port = "8080"
	}

	database, err := repository.ConnectDatabase(databaseDSN, config.GetDatabasePoolConfig())
	if err != nil {
		log.Fatalf("Failed to connect to MySQL: %v", err)
	}
	defer func() { _ = database.Close() }()

	transactionManager := repository.NewMySQLTransactionManager(database)
	userRepository := repository.NewMySQLUserRepository(database)
	smsRepository := repository.NewMySQLSMSRepository(database)
	creditRepository := repository.NewMySQLCreditRepository(database)

	redisRepository := repository.NewRedisRepository(redisAddress)
	producer := kafka.NewProducer(kafkaBrokers)
	defer producer.Close()

	handler := delivery.NewHandler(transactionManager, userRepository, smsRepository, creditRepository, redisRepository, producer)

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
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)
	<-signalChannel

	log.Println("Shutting down API server...")
	shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownContext); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}
}
