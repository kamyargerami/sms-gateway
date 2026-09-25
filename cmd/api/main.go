package main

import (
	"log"
	"os"

	"sms/internal/delivery"
	"sms/internal/kafka"
	"sms/internal/repository"

	"github.com/gin-gonic/gin"
)

func main() {
	dbDsn := os.Getenv("DB_DSN")
	redisAddr := os.Getenv("REDIS_ADDR")
	kafkaBrokers := []string{os.Getenv("KAFKA_BROKERS")}
	port := os.Getenv("SERVER_PORT")

	if port == "" {
		port = "8080"
	}

	db, err := repository.ConnectDB(dbDsn)
	if err != nil {
		log.Fatalf("Failed to connect to MySQL: %v", err)
	}

	transactionManager := repository.NewMySQLTransactionManager(db)
	userRepo := repository.NewMySQLUserRepository(db)
	smsRepo := repository.NewMySQLSMSRepository(db)
	transactionRepo := repository.NewMySQLTransactionRepository(db)

	redisRepo := repository.NewRedisRepository(redisAddr)
	producer := kafka.NewProducer(kafkaBrokers)
	defer producer.Close()

	handler := delivery.NewHandler(transactionManager, userRepo, smsRepo, transactionRepo, redisRepo, producer)

	r := gin.Default()

	v1 := r.Group("/api/v1")
	{
		v1.POST("/users/charge", handler.TopUp)
		v1.POST("/sms/send", handler.SendSMS)
		v1.GET("/users/:user_id/report", handler.GetReports)
	}

	log.Printf("Starting API server on port %s", port)
	if err := r.Run(":" + port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
