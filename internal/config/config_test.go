package config

import (
	"testing"
	"time"
)

func TestGetDBPoolConfig_FromEnv(t *testing.T) {
	t.Setenv("DB_MAX_OPEN_CONNS", "20")
	t.Setenv("DB_MAX_IDLE_CONNS", "8")
	t.Setenv("DB_CONN_MAX_LIFETIME", "2m")

	pool := GetDBPoolConfig()
	if pool.MaxOpenConns != 20 || pool.MaxIdleConns != 8 || pool.ConnMaxLifetime != 2*time.Minute {
		t.Fatalf("unexpected pool config: %+v", pool)
	}
}

func TestGetDBPoolConfig_Defaults(t *testing.T) {
	t.Setenv("DB_MAX_OPEN_CONNS", "")
	t.Setenv("DB_MAX_IDLE_CONNS", "invalid")
	t.Setenv("DB_CONN_MAX_LIFETIME", "")

	pool := GetDBPoolConfig()
	if pool.MaxOpenConns != 50 || pool.MaxIdleConns != 25 || pool.ConnMaxLifetime != 5*time.Minute {
		t.Fatalf("unexpected defaults: %+v", pool)
	}
}

func TestGetDBPoolConfig_IdleCappedAtOpen(t *testing.T) {
	t.Setenv("DB_MAX_OPEN_CONNS", "5")
	t.Setenv("DB_MAX_IDLE_CONNS", "50")

	if pool := GetDBPoolConfig(); pool.MaxIdleConns != 5 {
		t.Fatalf("idle conns should be capped at open conns, got %d", pool.MaxIdleConns)
	}
}
