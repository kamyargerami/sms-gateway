package operator

import (
	"testing"
)

func TestMock_SendSMS(testingT *testing.T) {
	operatorMock := NewMock()

	successCount := 0
	runs := 100

	for i := 0; i < runs; i++ {
		if operatorMock.SendSMS("09123456789", "Test") {
			successCount++
		}
	}

	// Mock operator has a 90% success rate, so we expect mostly successes.
	// We use 50 as a very safe lower bound to prevent flaky tests.
	if successCount < 50 {
		testingT.Errorf("Expected mostly successes, got %d out of %d", successCount, runs)
	}
}
