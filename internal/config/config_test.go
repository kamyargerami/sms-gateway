package config

import (
	"testing"
	"time"
)

func TestGetDatabasePoolConfig_FromEnvironment(testingT *testing.T) {
	testingT.Setenv("DATABASE_MAX_OPEN_CONNECTIONS", "20")
	testingT.Setenv("DATABASE_MAX_IDLE_CONNECTIONS", "8")
	testingT.Setenv("DATABASE_CONNECTION_MAX_LIFETIME", "2m")

	pool := GetDatabasePoolConfig()
	if pool.MaxOpenConnections != 20 || pool.MaxIdleConnections != 8 || pool.ConnectionMaxLifetime != 2*time.Minute {
		testingT.Fatalf("unexpected pool config: %+v", pool)
	}
}

func TestGetDatabasePoolConfig_Defaults(testingT *testing.T) {
	testingT.Setenv("DATABASE_MAX_OPEN_CONNECTIONS", "")
	testingT.Setenv("DATABASE_MAX_IDLE_CONNECTIONS", "invalid")
	testingT.Setenv("DATABASE_CONNECTION_MAX_LIFETIME", "")

	pool := GetDatabasePoolConfig()
	if pool.MaxOpenConnections != 50 || pool.MaxIdleConnections != 25 || pool.ConnectionMaxLifetime != 5*time.Minute {
		testingT.Fatalf("unexpected defaults: %+v", pool)
	}
}

func TestGetDatabasePoolConfig_IdleCappedAtOpen(testingT *testing.T) {
	testingT.Setenv("DATABASE_MAX_OPEN_CONNECTIONS", "5")
	testingT.Setenv("DATABASE_MAX_IDLE_CONNECTIONS", "50")

	if pool := GetDatabasePoolConfig(); pool.MaxIdleConnections != 5 {
		testingT.Fatalf("idle connections should be capped at open connections, got %d", pool.MaxIdleConnections)
	}
}

func TestGetExpressSMSTTL(testingT *testing.T) {
	testingT.Setenv("EXPRESS_SMS_TTL", "90s")
	if ttl := GetExpressSMSTTL(); ttl != 90*time.Second {
		testingT.Errorf("expected 90s, got %v", ttl)
	}
	for _, value := range []string{"", "garbage", "0", "-1m"} {
		testingT.Setenv("EXPRESS_SMS_TTL", value)
		if ttl := GetExpressSMSTTL(); ttl != 2*time.Minute {
			testingT.Errorf("EXPRESS_SMS_TTL=%q: expected default 2m, got %v", value, ttl)
		}
	}
}
