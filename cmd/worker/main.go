package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"sms/internal/config"
	"sms/internal/kafka"
	"sms/internal/operator"
	"sms/internal/repository"
)

func main() {
	dbDsn := os.Getenv("DB_DSN")
	redisAddr := os.Getenv("REDIS_ADDR")
	kafkaBrokers := config.GetKafkaBrokers()

	db, err := repository.ConnectDB(dbDsn, config.GetDBPoolConfig())
	if err != nil {
		log.Fatalf("Failed to connect to MySQL: %v", err)
	}

	transactionManager := repository.NewMySQLTransactionManager(db)
	userRepo := repository.NewMySQLUserRepository(db)
	smsRepo := repository.NewMySQLSMSRepository(db)
	creditRepo := repository.NewMySQLCreditRepository(db)
	redisRepo := repository.NewRedisRepository(redisAddr)

	operatorService := operator.NewMock()

	workerType := os.Getenv("WORKER_TYPE")
	if workerType != "" && workerType != "express" && workerType != "bulk" {
		log.Fatalf("Invalid WORKER_TYPE %q (expected express, bulk or empty)", workerType)
	}

	goContext, cancel := context.WithCancel(context.Background())
	var waitGroup sync.WaitGroup

	log.Printf("Starting SMS Workers... (Mode: %s)\n", workerType)

	var expressConsumer, bulkConsumer *kafka.Consumer

	if workerType == "express" || workerType == "" {
		expressConsumer = kafka.NewConsumer(
			kafkaBrokers, "sms_express", "worker-group-express",
			transactionManager, userRepo, smsRepo, creditRepo, redisRepo, operatorService,
		)
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			expressConsumer.Start(goContext)
		}()
	}

	if workerType == "bulk" || workerType == "" {
		bulkConsumer = kafka.NewConsumer(
			kafkaBrokers, "sms_bulk", "worker-group-bulk",
			transactionManager, userRepo, smsRepo, creditRepo, redisRepo, operatorService,
		)
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			bulkConsumer.Start(goContext)
		}()
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down workers...")
	cancel()
	// Wait for in-flight messages to finish (and commit) BEFORE closing the readers;
	// closing first makes the final CommitMessages calls fail.
	waitGroup.Wait()

	if expressConsumer != nil {
		if err := expressConsumer.Close(); err != nil {
			log.Printf("Error closing express consumer: %v\n", err)
		}
	}
	if bulkConsumer != nil {
		if err := bulkConsumer.Close(); err != nil {
			log.Printf("Error closing bulk consumer: %v\n", err)
		}
	}
	_ = db.Close()

	log.Println("Workers stopped gracefully")
}
