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
	mutex     sync.Mutex
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

func (store *fakeStore) takeSnapshot() snapshot {
	balances := map[int]int{}
	for userID, balance := range store.balances {
		balances[userID] = balance
	}
	sms := map[string]domain.SMS{}
	for smsID, record := range store.sms {
		sms[smsID] = record
	}
	return snapshot{balances, sms, append([]string(nil), store.credits...)}
}

func (store *fakeStore) WithTransaction(goContext context.Context, operation func(context.Context) error) error {
	store.mutex.Lock()
	if store.failNextN > 0 {
		store.failNextN--
		store.mutex.Unlock()
		return errors.New("transient database error")
	}
	before := store.takeSnapshot()
	store.mutex.Unlock()

	if err := operation(goContext); err != nil {
		store.mutex.Lock()
		store.balances, store.sms, store.credits = before.balances, before.sms, before.credits
		store.mutex.Unlock()
		return err
	}
	return nil
}

func (store *fakeStore) GetBalance(goContext context.Context, userID int) (int, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	balance, ok := store.balances[userID]
	if !ok {
		return 0, domain.ErrUserNotFound
	}
	return balance, nil
}

func (store *fakeStore) AddBalance(goContext context.Context, userID, amount int) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if _, ok := store.balances[userID]; !ok {
		return domain.ErrUserNotFound
	}
	store.balances[userID] += amount
	return nil
}

func (store *fakeStore) DeductBalance(goContext context.Context, userID, amount int) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()
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

type fakeSMSRepository struct{ store *fakeStore }

func (repository fakeSMSRepository) Create(goContext context.Context, sms *domain.SMS) error {
	repository.store.mutex.Lock()
	defer repository.store.mutex.Unlock()
	if _, exists := repository.store.sms[sms.ID]; exists {
		return domain.ErrDuplicateRecord
	}
	repository.store.sms[sms.ID] = *sms
	return nil
}

func (repository fakeSMSRepository) UpdateStatusFrom(goContext context.Context, id, from, to string) error {
	repository.store.mutex.Lock()
	defer repository.store.mutex.Unlock()
	record, ok := repository.store.sms[id]
	if !ok || record.Status != from {
		return domain.ErrStatusNotChanged
	}
	record.Status = to
	repository.store.sms[id] = record
	return nil
}

func (repository fakeSMSRepository) GetByID(goContext context.Context, id string) (*domain.SMS, error) {
	repository.store.mutex.Lock()
	defer repository.store.mutex.Unlock()
	record, ok := repository.store.sms[id]
	if !ok {
		return nil, domain.ErrSMSNotFound
	}
	return &record, nil
}

func (repository fakeSMSRepository) GetByUserID(goContext context.Context, userID int) ([]domain.SMS, error) {
	return nil, nil
}

type fakeCreditRepository struct{ store *fakeStore }

func (repository fakeCreditRepository) Create(goContext context.Context, userID, amount int, creditType string) error {
	repository.store.mutex.Lock()
	defer repository.store.mutex.Unlock()
	repository.store.credits = append(repository.store.credits, creditType)
	return nil
}

type fakeCache struct {
	added         int
	invalidations int
}

func (cache *fakeCache) AddBalance(goContext context.Context, userID, amount int) error {
	cache.added += amount
	return nil
}
func (cache *fakeCache) DeductBalance(goContext context.Context, userID, amount int) (int, error) {
	return 1, nil
}
func (cache *fakeCache) InitBalance(goContext context.Context, userID, balance int) error { return nil }
func (cache *fakeCache) InvalidateBalance(goContext context.Context, userID int) error {
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
		userRepository:     store,
		smsRepository:      fakeSMSRepository{store},
		creditRepository:   fakeCreditRepository{store},
		cache:              cache,
		topic:              "sms_test",
		retryBackoff:       time.Millisecond,
	}
}

func smsMessage(testingT *testing.T, id string) kafka.Message {
	return encodeSMS(testingT, domain.SMS{ID: id, UserID: 1, ToNumber: "09123456789", Text: "hi"})
}

func encodeSMS(testingT *testing.T, sms domain.SMS) kafka.Message {
	body, err := json.Marshal(sms)
	require.NoError(testingT, err)
	return kafka.Message{Value: body}
}

// expiringSMSMessage is an SMS created `age` ago that expires `ttl` after creation.
func expiringSMSMessage(testingT *testing.T, id string, age, ttl time.Duration) kafka.Message {
	createdAt := time.Now().Add(-age)
	expiresAt := createdAt.Add(ttl)
	return encodeSMS(testingT, domain.SMS{ID: id, UserID: 1, ToNumber: "09123456789", Text: "otp", IsExpress: true, CreatedAt: createdAt, ExpiresAt: &expiresAt})
}

func TestConsumer_Delivered(testingT *testing.T) {
	testingT.Setenv("SMS_COST", "10")
	store, operator, cache := newFakeStore(100), &fakeOperator{success: true}, &fakeCache{}
	consumer := newTestConsumer(store, operator, cache)

	require.NoError(testingT, consumer.processMessage(context.Background(), smsMessage(testingT, "a")))

	assert.Equal(testingT, 90, store.balances[1])
	assert.Equal(testingT, domain.StatusDelivered, store.sms["a"].Status)
	assert.Equal(testingT, 1, operator.calls)
}

func TestConsumer_OperatorFailureRefundsOnce(testingT *testing.T) {
	testingT.Setenv("SMS_COST", "10")
	store, operator, cache := newFakeStore(100), &fakeOperator{success: false}, &fakeCache{}
	consumer := newTestConsumer(store, operator, cache)

	require.NoError(testingT, consumer.processMessage(context.Background(), smsMessage(testingT, "a")))
	// Redelivery of the same message must not refund (or send) again.
	require.NoError(testingT, consumer.processMessage(context.Background(), smsMessage(testingT, "a")))

	assert.Equal(testingT, 100, store.balances[1])
	assert.Equal(testingT, domain.StatusFailed, store.sms["a"].Status)
	assert.Equal(testingT, []string{domain.CreditSMSSent, domain.CreditRefund}, store.credits)
	assert.Equal(testingT, 10, cache.added)
	assert.Equal(testingT, 1, operator.calls)
}

func TestConsumer_InsufficientBalanceNeverSends(testingT *testing.T) {
	testingT.Setenv("SMS_COST", "10")
	store, operator, cache := newFakeStore(5), &fakeOperator{success: true}, &fakeCache{}
	consumer := newTestConsumer(store, operator, cache)

	require.NoError(testingT, consumer.processMessage(context.Background(), smsMessage(testingT, "a")))

	assert.Equal(testingT, 5, store.balances[1], "balance must never go below zero")
	assert.Equal(testingT, 0, operator.calls, "no free SMS")
	assert.Equal(testingT, domain.StatusFailed, store.sms["a"].Status)
	assert.Equal(testingT, 1, cache.invalidations)
}

func TestConsumer_RedeliveryOfPaidPendingSMSResumes(testingT *testing.T) {
	testingT.Setenv("SMS_COST", "10")
	// Simulate a worker that debited + inserted, then crashed before sending,
	// and the user's remaining balance is now 0.
	store, operator, cache := newFakeStore(0), &fakeOperator{success: true}, &fakeCache{}
	store.sms["a"] = domain.SMS{ID: "a", UserID: 1, Status: domain.StatusPending}
	consumer := newTestConsumer(store, operator, cache)

	require.NoError(testingT, consumer.processMessage(context.Background(), smsMessage(testingT, "a")))

	assert.Equal(testingT, 0, store.balances[1], "must not debit twice")
	assert.Equal(testingT, 1, operator.calls, "already paid, must still be sent")
	assert.Equal(testingT, domain.StatusDelivered, store.sms["a"].Status)
}

func TestConsumer_TransientDBErrorIsRetriedNotSkipped(testingT *testing.T) {
	testingT.Setenv("SMS_COST", "10")
	store, operator, cache := newFakeStore(100), &fakeOperator{success: true}, &fakeCache{}
	store.failNextN = 3
	consumer := newTestConsumer(store, operator, cache)

	require.NoError(testingT, consumer.processMessage(context.Background(), smsMessage(testingT, "a")))

	assert.Equal(testingT, domain.StatusDelivered, store.sms["a"].Status)
	assert.Equal(testingT, 90, store.balances[1])
}

func TestConsumer_ShutdownDuringRetryDoesNotCommit(testingT *testing.T) {
	testingT.Setenv("SMS_COST", "10")
	store, operator, cache := newFakeStore(100), &fakeOperator{success: true}, &fakeCache{}
	store.failNextN = 1 << 30 // DB permanently down
	consumer := newTestConsumer(store, operator, cache)

	goContext, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := consumer.processMessage(goContext, smsMessage(testingT, "a"))
	assert.Error(testingT, err, "caller must not commit the offset")
	assert.Equal(testingT, 0, operator.calls)
}

func TestConsumer_ExpiredBeforePickupIsNeitherSentNorCharged(testingT *testing.T) {
	testingT.Setenv("SMS_COST", "10")
	store, operator, cache := newFakeStore(100), &fakeOperator{success: true}, &fakeCache{}
	consumer := newTestConsumer(store, operator, cache)

	// Created 5 minutes ago with a 2 minute TTL: sat in the queue past its deadline.
	message := expiringSMSMessage(testingT, "a", 5*time.Minute, 2*time.Minute)
	require.NoError(testingT, consumer.processMessage(context.Background(), message))

	assert.Equal(testingT, 0, operator.calls, "an expired OTP must not be sent")
	assert.Equal(testingT, domain.StatusExpired, store.sms["a"].Status)
	assert.Equal(testingT, 100, store.balances[1], "MySQL must never be debited")
	assert.Empty(testingT, store.credits)
	assert.Equal(testingT, 10, cache.added, "the credit reserved in Redis by the API is given back")

	// Redelivery must not refund Redis a second time.
	require.NoError(testingT, consumer.processMessage(context.Background(), message))
	assert.Equal(testingT, 10, cache.added)
}

func TestConsumer_ExpiredRedeliveryOfPaidPendingSMSIsRefunded(testingT *testing.T) {
	testingT.Setenv("SMS_COST", "10")
	store, operator, cache := newFakeStore(90), &fakeOperator{success: true}, &fakeCache{}
	// A previous worker debited and inserted the SMS, then crashed before sending;
	// by the time it is redelivered the deadline has passed.
	store.sms["a"] = domain.SMS{ID: "a", UserID: 1, Status: domain.StatusPending}
	store.credits = []string{domain.CreditSMSSent}
	consumer := newTestConsumer(store, operator, cache)

	message := expiringSMSMessage(testingT, "a", 5*time.Minute, 2*time.Minute)
	require.NoError(testingT, consumer.processMessage(context.Background(), message))

	assert.Equal(testingT, 0, operator.calls)
	assert.Equal(testingT, domain.StatusExpired, store.sms["a"].Status)
	assert.Equal(testingT, 100, store.balances[1], "paid by the crashed attempt, so refunded")
	assert.Equal(testingT, []string{domain.CreditSMSSent, domain.CreditRefund}, store.credits)
	assert.Equal(testingT, 10, cache.added)
}

func TestConsumer_ExpiresWhileDebitIsRetriedIsRefundedNotSent(testingT *testing.T) {
	testingT.Setenv("SMS_COST", "10")
	store, operator, cache := newFakeStore(100), &fakeOperator{success: true}, &fakeCache{}
	consumer := newTestConsumer(store, operator, cache)

	// The clock jumps past the deadline after the pickup check, as if the debit
	// transaction had been retrying against a slow database.
	createdAt := time.Now()
	expiresAt := createdAt.Add(time.Minute)
	clockCalls := 0
	consumer.now = func() time.Time {
		clockCalls++
		if clockCalls == 1 {
			return createdAt
		}
		return expiresAt.Add(time.Second)
	}

	message := encodeSMS(testingT, domain.SMS{ID: "a", UserID: 1, ToNumber: "09123456789", Text: "otp", IsExpress: true, CreatedAt: createdAt, ExpiresAt: &expiresAt})
	require.NoError(testingT, consumer.processMessage(context.Background(), message))

	assert.Equal(testingT, 0, operator.calls)
	assert.Equal(testingT, domain.StatusExpired, store.sms["a"].Status)
	assert.Equal(testingT, 100, store.balances[1])
	assert.Equal(testingT, []string{domain.CreditSMSSent, domain.CreditRefund}, store.credits)
	assert.Equal(testingT, 10, cache.added)
}

func TestConsumer_NotYetExpiredIsSent(testingT *testing.T) {
	testingT.Setenv("SMS_COST", "10")
	store, operator, cache := newFakeStore(100), &fakeOperator{success: true}, &fakeCache{}
	consumer := newTestConsumer(store, operator, cache)

	message := expiringSMSMessage(testingT, "a", time.Second, 2*time.Minute)
	require.NoError(testingT, consumer.processMessage(context.Background(), message))

	assert.Equal(testingT, 1, operator.calls)
	assert.Equal(testingT, domain.StatusDelivered, store.sms["a"].Status)
	assert.Equal(testingT, 90, store.balances[1])
}
