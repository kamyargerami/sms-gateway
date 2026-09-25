package repository

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type RedisRepository struct {
	client *redis.Client
}

func NewRedisRepository(address string) *RedisRepository {
	redisClient := redis.NewClient(&redis.Options{
		Addr: address,
	})
	return &RedisRepository{client: redisClient}
}

func balanceKey(userID int) string {
	return fmt.Sprintf("user_balance:%d", userID)
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

// Scripts are cached server-side and invoked via EVALSHA instead of sending the
// full script body on every request.
var deductScript = redis.NewScript(luaScript)

// DeductBalance atomic deduction using Lua script
func (repository *RedisRepository) DeductBalance(goContext context.Context, userID int, cost int) (int, error) {
	key := balanceKey(userID)

	result, err := deductScript.Run(goContext, repository.client, []string{key}, cost).Result()
	if err != nil {
		return 0, err
	}

	resultValue, ok := result.(int64)
	if !ok {
		return 0, fmt.Errorf("unexpected lua result type")
	}

	return int(resultValue), nil
}

// InitBalance loads the balance into Redis only if the key does not exist yet (SET NX).
// A plain SET here would let two concurrent cache-miss requests overwrite a
// deduction that already happened, effectively granting free SMS.
func (repository *RedisRepository) InitBalance(goContext context.Context, userID int, balance int) error {
	return repository.client.SetNX(goContext, balanceKey(userID), balance, 0).Err()
}

// InvalidateBalance removes the cached balance so it is reloaded from MySQL.
func (repository *RedisRepository) InvalidateBalance(goContext context.Context, userID int) error {
	return repository.client.Del(goContext, balanceKey(userID)).Err()
}

// luaAddScript atomically increments if key exists.
const luaAddScript = `
if redis.call("EXISTS", KEYS[1]) == 1 then
    return redis.call("INCRBY", KEYS[1], tonumber(ARGV[1]))
end
return 0
`

var addScript = redis.NewScript(luaAddScript)

// AddBalance increments the balance in Redis atomically
func (repository *RedisRepository) AddBalance(goContext context.Context, userID int, amount int) error {
	key := balanceKey(userID)

	_, err := addScript.Run(goContext, repository.client, []string{key}, amount).Result()
	return err
}
