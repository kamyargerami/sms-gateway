package operator

import (
	"math/rand"
	"time"
)

type Mock struct{}

func NewMock() *Mock {
	// Initialize random seed
	rand.Seed(time.Now().UnixNano())
	return &Mock{}
}

// SendSMS simulates network latency and random success/failure (90% success rate)
func (m *Mock) SendSMS(to string, text string) bool {
	// Simulate latency between 10ms to 100ms
	latency := rand.Intn(90) + 10
	time.Sleep(time.Duration(latency) * time.Millisecond)

	// 90% chance of success
	return rand.Intn(100) < 90
}
