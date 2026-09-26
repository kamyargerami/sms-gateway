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

// DatabasePoolConfig holds the database/sql connection-pool limits.
type DatabasePoolConfig struct {
	MaxOpenConnections    int
	MaxIdleConnections    int
	ConnectionMaxLifetime time.Duration
}

// GetDatabasePoolConfig reads DATABASE_MAX_OPEN_CONNECTIONS, DATABASE_MAX_IDLE_CONNECTIONS and
// DATABASE_CONNECTION_MAX_LIFETIME (a Go duration such as "5m"). Keep
// (api replicas + worker replicas) * DATABASE_MAX_OPEN_CONNECTIONS below MySQL's max_connections.
func GetDatabasePoolConfig() DatabasePoolConfig {
	pool := DatabasePoolConfig{
		MaxOpenConnections:    getPositiveIntFromEnvironment("DATABASE_MAX_OPEN_CONNECTIONS", 50),
		ConnectionMaxLifetime: 5 * time.Minute,
	}
	pool.MaxIdleConnections = getPositiveIntFromEnvironment("DATABASE_MAX_IDLE_CONNECTIONS", pool.MaxOpenConnections/2)
	if pool.MaxIdleConnections > pool.MaxOpenConnections {
		pool.MaxIdleConnections = pool.MaxOpenConnections
	}
	if lifetime, err := time.ParseDuration(os.Getenv("DATABASE_CONNECTION_MAX_LIFETIME")); err == nil && lifetime > 0 {
		pool.ConnectionMaxLifetime = lifetime
	}
	return pool
}

func getPositiveIntFromEnvironment(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

const defaultExpressSMSTTL = 2 * time.Minute

// GetExpressSMSTTL returns the delivery deadline for express SMS (almost always an
// OTP, which is worthless after a couple of minutes), read from EXPRESS_SMS_TTL as a
// Go duration such as "2m". An express SMS still queued after created_at + TTL is
// marked EXPIRED instead of being sent, and is not charged. Bulk SMS have no
// deadline. Missing, invalid or non-positive values fall back to 2m.
func GetExpressSMSTTL() time.Duration {
	ttl, err := time.ParseDuration(strings.TrimSpace(os.Getenv("EXPRESS_SMS_TTL")))
	if err != nil || ttl <= 0 {
		return defaultExpressSMSTTL
	}
	return ttl
}
