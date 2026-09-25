package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"sms/internal/domain"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeStore is an in-memory stand-in for MySQL with real rollback semantics,
// so tests can verify the consumer's transactional guarantees.
type fakeStore struct {
	mu        sync.Mutex
	balances  map[int]int
	sms       map[string]domain.SMS
	credits   []string
	failNextN int // number of upcoming transactions that fail with a transient error
}

type snapshot struct {
	balances map[int]int
	sms      map[string]domain.SMS
	credits  []string
}

func newFakeStore(balance int) *fakeStore {
	return &fakeStore{balances: map[int]int{1: balance}, sms: map[string]domain.SMS{}}
}

func (store *fakeStore) snap() snapshot {
	balances := map[int]int{}
	for k, v := range store.balances {
		balances[k] = v
	}
	sms := map[string]domain.SMS{}
	for k, v := range store.sms {
		sms[k] = v
	}
	return snapshot{balances, sms, append([]string(nil), store.credits...)}
}

func (store *fakeStore) WithTransaction(ctx context.Context, fn func(context.Context) error) error {
	store.mu.Lock()
	if store.failNextN > 0 {
		store.failNextN--
		store.mu.Unlock()
		return errors.New("transient db error")
	}
	before := store.snap()
	store.mu.Unlock()

	if err := fn(ctx); err != nil {
		store.mu.Lock()
		store.balances, store.sms, store.credits = before.balances, before.sms, before.credits
		store.mu.Unlock()
		return err
	}
	return nil
}

func (store *fakeStore) GetBalance(ctx context.Context, userID int) (int, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	balance, ok := store.balances[userID]
	if !ok {
		return 0, domain.ErrUserNotFound
	}
	return balance, nil
}

func (store *fakeStore) AddBalance(ctx context.Context, userID, amount int) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, ok := store.balances[userID]; !ok {
		return domain.ErrUserNotFound
	}
	store.balances[userID] += amount
	return nil
}

func (store *fakeStore) DeductBalance(ctx context.Context, userID, amount int) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	balance, ok := store.balances[userID]
	if !ok {
		return domain.ErrUserNotFound
	}
	if balance < amount {
		return domain.ErrInsufficientBalance
	}
	store.balances[userID] = balance - amount
	return nil
}

type fakeSMSRepo struct{ store *fakeStore }

func (repo fakeSMSRepo) Create(ctx context.Context, sms *domain.SMS) error {
	repo.store.mu.Lock()
	defer repo.store.mu.Unlock()
	if _, exists := repo.store.sms[sms.ID]; exists {
		return domain.ErrDuplicateRecord
	}
	repo.store.sms[sms.ID] = *sms
	return nil
}

func (repo fakeSMSRepo) UpdateStatusFrom(ctx context.Context, id, from, to string) error {
	repo.store.mu.Lock()
	defer repo.store.mu.Unlock()
	record, ok := repo.store.sms[id]
	if !ok || record.Status != from {
		return domain.ErrStatusNotChanged
	}
	record.Status = to
	repo.store.sms[id] = record
	return nil
}

func (repo fakeSMSRepo) GetByID(ctx context.Context, id string) (*domain.SMS, error) {
	repo.store.mu.Lock()
	defer repo.store.mu.Unlock()
	record, ok := repo.store.sms[id]
	if !ok {
		return nil, domain.ErrSMSNotFound
	}
	return &record, nil
}

func (repo fakeSMSRepo) GetByUserID(ctx context.Context, userID int) ([]domain.SMS, error) {
	return nil, nil
}

type fakeCreditRepo struct{ store *fakeStore }

func (repo fakeCreditRepo) Create(ctx context.Context, userID, amount int, creditType string) error {
	repo.store.mu.Lock()
	defer repo.store.mu.Unlock()
	repo.store.credits = append(repo.store.credits, creditType)
	return nil
}

type fakeCache struct {
	added         int
	invalidations int
}

func (cache *fakeCache) AddBalance(ctx context.Context, userID, amount int) error {
	cache.added += amount
	return nil
}
func (cache *fakeCache) DeductBalance(ctx context.Context, userID, amount int) (int, error) {
	return 1, nil
}
func (cache *fakeCache) InitBalance(ctx context.Context, userID, balance int) error { return nil }
func (cache *fakeCache) InvalidateBalance(ctx context.Context, userID int) error {
	cache.invalidations++
	return nil
}

type fakeOperator struct {
	success bool
	calls   int
}

func (operator *fakeOperator) SendSMS(to, text string) bool {
	operator.calls++
	return operator.success
}

func newTestConsumer(store *fakeStore, operator *fakeOperator, cache *fakeCache) *Consumer {
	return &Consumer{
		operator:           operator,
		transactionManager: store,
		userRepo:           store,
		smsRepo:            fakeSMSRepo{store},
		creditRepo:         fakeCreditRepo{store},
		cache:              cache,
		retryBackoff:       time.Millisecond,
	}
}

func smsMessage(t *testing.T, id string) kafka.Message {
	body, err := json.Marshal(domain.SMS{ID: id, UserID: 1, ToNumber: "09123456789", Text: "hi"})
	require.NoError(t, err)
	return kafka.Message{Value: body}
}

func TestConsumer_Delivered(t *testing.T) {
	t.Setenv("SMS_COST", "10")
	store, operator, cache := newFakeStore(100), &fakeOperator{success: true}, &fakeCache{}
	consumer := newTestConsumer(store, operator, cache)

	require.NoError(t, consumer.processMessage(context.Background(), smsMessage(t, "a")))

	assert.Equal(t, 90, store.balances[1])
	assert.Equal(t, domain.StatusDelivered, store.sms["a"].Status)
	assert.Equal(t, 1, operator.calls)
}

func TestConsumer_OperatorFailureRefundsOnce(t *testing.T) {
	t.Setenv("SMS_COST", "10")
	store, operator, cache := newFakeStore(100), &fakeOperator{success: false}, &fakeCache{}
	consumer := newTestConsumer(store, operator, cache)

	require.NoError(t, consumer.processMessage(context.Background(), smsMessage(t, "a")))
	// Redelivery of the same message must not refund (or send) again.
	require.NoError(t, consumer.processMessage(context.Background(), smsMessage(t, "a")))

	assert.Equal(t, 100, store.balances[1])
	assert.Equal(t, domain.StatusFailed, store.sms["a"].Status)
	assert.Equal(t, []string{domain.CreditSMSSent, domain.CreditRefund}, store.credits)
	assert.Equal(t, 10, cache.added)
	assert.Equal(t, 1, operator.calls)
}

func TestConsumer_InsufficientBalanceNeverSends(t *testing.T) {
	t.Setenv("SMS_COST", "10")
	store, operator, cache := newFakeStore(5), &fakeOperator{success: true}, &fakeCache{}
	consumer := newTestConsumer(store, operator, cache)

	require.NoError(t, consumer.processMessage(context.Background(), smsMessage(t, "a")))

	assert.Equal(t, 5, store.balances[1], "balance must never go below zero")
	assert.Equal(t, 0, operator.calls, "no free SMS")
	assert.Equal(t, domain.StatusFailed, store.sms["a"].Status)
	assert.Equal(t, 1, cache.invalidations)
}

func TestConsumer_RedeliveryOfPaidPendingSMSResumes(t *testing.T) {
	t.Setenv("SMS_COST", "10")
	// Simulate a worker that debited + inserted, then crashed before sending,
	// and the user's remaining balance is now 0.
	store, operator, cache := newFakeStore(0), &fakeOperator{success: true}, &fakeCache{}
	store.sms["a"] = domain.SMS{ID: "a", UserID: 1, Status: domain.StatusPending}
	consumer := newTestConsumer(store, operator, cache)

	require.NoError(t, consumer.processMessage(context.Background(), smsMessage(t, "a")))

	assert.Equal(t, 0, store.balances[1], "must not debit twice")
	assert.Equal(t, 1, operator.calls, "already paid, must still be sent")
	assert.Equal(t, domain.StatusDelivered, store.sms["a"].Status)
}

func TestConsumer_TransientDBErrorIsRetriedNotSkipped(t *testing.T) {
	t.Setenv("SMS_COST", "10")
	store, operator, cache := newFakeStore(100), &fakeOperator{success: true}, &fakeCache{}
	store.failNextN = 3
	consumer := newTestConsumer(store, operator, cache)

	require.NoError(t, consumer.processMessage(context.Background(), smsMessage(t, "a")))

	assert.Equal(t, domain.StatusDelivered, store.sms["a"].Status)
	assert.Equal(t, 90, store.balances[1])
}

func TestConsumer_ShutdownDuringRetryDoesNotCommit(t *testing.T) {
	t.Setenv("SMS_COST", "10")
	store, operator, cache := newFakeStore(100), &fakeOperator{success: true}, &fakeCache{}
	store.failNextN = 1 << 30 // DB permanently down
	consumer := newTestConsumer(store, operator, cache)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := consumer.processMessage(ctx, smsMessage(t, "a"))
	assert.Error(t, err, "caller must not commit the offset")
	assert.Equal(t, 0, operator.calls)
}
