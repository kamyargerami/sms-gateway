package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"sms/internal/kafka"
	"sms/internal/operator"
	"sms/internal/repository"
)

func main() {
	dbDsn := os.Getenv("DB_DSN")
	redisAddr := os.Getenv("REDIS_ADDR")
	kafkaBrokers := []string{os.Getenv("KAFKA_BROKERS")}

	db, err := repository.ConnectDB(dbDsn)
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

	waitGroup.Wait()
	log.Println("Workers stopped gracefully")
}
