package utils

import (
	"errors"
	"fmt"
	"io"
)

// RDBError represents an error in RDB parsing
type RDBError struct {
	Operation string
	Err       error
}

// NewRDBError creates a new RDB error
func NewRDBError(operation string, err error) *RDBError {
	return &RDBError{
		Operation: operation,
		Err:       err,
	}
}

// Error implements the error interface
func (e *RDBError) Error() string {
	return fmt.Sprintf("RDB error in %s: %v", e.Operation, e.Err)
}

// Unwrap returns the underlying error
func (e *RDBError) Unwrap() error {
	return e.Err
}

// IsEOF checks if the error is EOF
func IsEOF(err error) bool {
	if err == io.EOF {
		return true
	}

	var rdbErr *RDBError
	if errors.As(err, &rdbErr) {
		return rdbErr.Err == io.EOF
	}

	return false
}
