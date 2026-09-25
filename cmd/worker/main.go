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
	databaseDSN := os.Getenv("DATABASE_DSN")
	redisAddress := os.Getenv("REDIS_ADDRESS")
	kafkaBrokers := config.GetKafkaBrokers()

	database, err := repository.ConnectDatabase(databaseDSN, config.GetDatabasePoolConfig())
	if err != nil {
		log.Fatalf("Failed to connect to MySQL: %v", err)
	}

	transactionManager := repository.NewMySQLTransactionManager(database)
	userRepository := repository.NewMySQLUserRepository(database)
	smsRepository := repository.NewMySQLSMSRepository(database)
	creditRepository := repository.NewMySQLCreditRepository(database)
	redisRepository := repository.NewRedisRepository(redisAddress)

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
			transactionManager, userRepository, smsRepository, creditRepository, redisRepository, operatorService,
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
			transactionManager, userRepository, smsRepository, creditRepository, redisRepository, operatorService,
		)
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			bulkConsumer.Start(goContext)
		}()
	}

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)
	<-signalChannel

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
	_ = database.Close()

	log.Println("Workers stopped gracefully")
}
