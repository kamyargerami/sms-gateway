package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

const defaultSMSCost = 10

// GetSMSCost returns the per-SMS cost. Non-positive or invalid values fall back to
// the default, since a cost <= 0 would let users send unlimited free SMS.
func GetSMSCost() int {
	cost, err := strconv.Atoi(os.Getenv("SMS_COST"))
	if err != nil || cost <= 0 {
		return defaultSMSCost
	}
	return cost
}

// GetKafkaBrokers parses a comma-separated KAFKA_BROKERS list ("host1:9092,host2:9092").
func GetKafkaBrokers() []string {
	var brokers []string
	for _, broker := range strings.Split(os.Getenv("KAFKA_BROKERS"), ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}

// DBPoolConfig holds the database/sql connection-pool limits.
type DBPoolConfig struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

// GetDBPoolConfig reads DB_MAX_OPEN_CONNS, DB_MAX_IDLE_CONNS and
// DB_CONN_MAX_LIFETIME (a Go duration such as "5m"). Keep
// (api replicas + worker replicas) * DB_MAX_OPEN_CONNS below MySQL's max_connections.
func GetDBPoolConfig() DBPoolConfig {
	pool := DBPoolConfig{
		MaxOpenConns:    getPositiveInt("DB_MAX_OPEN_CONNS", 50),
		ConnMaxLifetime: 5 * time.Minute,
	}
	pool.MaxIdleConns = getPositiveInt("DB_MAX_IDLE_CONNS", pool.MaxOpenConns/2)
	if pool.MaxIdleConns > pool.MaxOpenConns {
		pool.MaxIdleConns = pool.MaxOpenConns
	}
	if lifetime, err := time.ParseDuration(os.Getenv("DB_CONN_MAX_LIFETIME")); err == nil && lifetime > 0 {
		pool.ConnMaxLifetime = lifetime
	}
	return pool
}

func getPositiveInt(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
