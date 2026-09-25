package domain

import "errors"

var (
	ErrDuplicateRecord     = errors.New("duplicate record exists")
	ErrUserNotFound        = errors.New("user not found")
	ErrSMSNotFound         = errors.New("sms not found")
	ErrInsufficientBalance = errors.New("insufficient balance")
	// ErrStatusNotChanged is returned when a conditional status transition
	// (e.g. PENDING -> FAILED) did not apply because the record is no longer
	// in the expected state (already finalized by a previous delivery attempt).
	ErrStatusNotChanged = errors.New("sms status not changed")
)
