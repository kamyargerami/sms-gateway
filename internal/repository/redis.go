package repository

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type RedisRepository struct {
	client *redis.Client
}

func NewRedisRepository(addr string) *RedisRepository {
	redisClient := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &RedisRepository{client: redisClient}
}

// luaScript checks balance and decrements if sufficient.
// Returns 1 if success, 0 if insufficient, -1 if key does not exist.
const luaScript = `
local balance = redis.call("GET", KEYS[1])
if not balance then
    return -1
end
if tonumber(balance) >= tonumber(ARGV[1]) then
    redis.call("DECRBY", KEYS[1], tonumber(ARGV[1]))
    return 1
else
    return 0
end
`

// DeductBalance atomic deduction using Lua script
func (repository *RedisRepository) DeductBalance(goContext context.Context, userID int, cost int) (int, error) {
	key := fmt.Sprintf("user_balance:%d", userID)

	result, err := repository.client.Eval(goContext, luaScript, []string{key}, cost).Result()
	if err != nil {
		return 0, err
	}

	val, ok := result.(int64)
	if !ok {
		return 0, fmt.Errorf("unexpected lua result type")
	}

	return int(val), nil
}

// SetBalance initializes the balance in Redis
func (repository *RedisRepository) SetBalance(goContext context.Context, userID int, balance int) error {
	key := fmt.Sprintf("user_balance:%d", userID)
	return repository.client.Set(goContext, key, balance, 0).Err()
}

// luaAddScript atomically increments if key exists.
const luaAddScript = `
if redis.call("EXISTS", KEYS[1]) == 1 then
    return redis.call("INCRBY", KEYS[1], tonumber(ARGV[1]))
end
return 0
`

// AddBalance increments the balance in Redis atomically
func (repository *RedisRepository) AddBalance(goContext context.Context, userID int, amount int) error {
	key := fmt.Sprintf("user_balance:%d", userID)

	_, err := repository.client.Eval(goContext, luaAddScript, []string{key}, amount).Result()
	return err
}
