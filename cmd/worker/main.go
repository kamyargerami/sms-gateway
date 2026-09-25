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
	transactionRepo := repository.NewMySQLTransactionRepository(db)
	redisRepo := repository.NewRedisRepository(redisAddr)

	op := operator.NewMock()

	workerType := os.Getenv("WORKER_TYPE")

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	log.Printf("Starting SMS Workers... (Mode: %s)\n", workerType)

	var expressConsumer, bulkConsumer *kafka.Consumer

	if workerType == "express" || workerType == "" {
		expressConsumer = kafka.NewConsumer(
			kafkaBrokers, "sms_express", "worker-group-express",
			transactionManager, userRepo, smsRepo, transactionRepo, redisRepo, op,
		)
		wg.Add(1)
		go func() {
			defer wg.Done()
			expressConsumer.Start(ctx)
		}()
	}

	if workerType == "bulk" || workerType == "" {
		bulkConsumer = kafka.NewConsumer(
			kafkaBrokers, "sms_bulk", "worker-group-bulk",
			transactionManager, userRepo, smsRepo, transactionRepo, redisRepo, op,
		)
		wg.Add(1)
		go func() {
			defer wg.Done()
			bulkConsumer.Start(ctx)
		}()
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down workers...")
	cancel()
	if expressConsumer != nil {
		expressConsumer.Close()
	}
	if bulkConsumer != nil {
		bulkConsumer.Close()
	}

	wg.Wait()
	log.Println("Workers stopped gracefully")
}
