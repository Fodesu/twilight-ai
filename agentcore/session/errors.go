package session

import (
	"errors"
	"fmt"
)

// ErrorCode classifies kernel failures (SES 7).
type ErrorCode string

const (
	ErrInvalid       ErrorCode = "invalid"
	ErrNotFound      ErrorCode = "not_found"
	ErrConflict      ErrorCode = "conflict"
	ErrCorrupt       ErrorCode = "corrupt"
	ErrOwned         ErrorCode = "owned"
	ErrOwnershipLost ErrorCode = "ownership_lost"
	// ErrReferenced: a segment removal found a live tip or a child edge
	// still naming the node; nothing was removed (SES-GC-4).
	ErrReferenced ErrorCode = "referenced"
	// ErrDeleted: the SessionID names a deleted Session; it is never reused
	// (SES-GC-1).
	ErrDeleted ErrorCode = "deleted"
	// ErrHandleFailed: a previous Append of this Handle failed while writing
	// or persisting, so what reached storage is unknown. The Handle refuses
	// further Appends; the caller reopens and Open reads the log as it is
	// (SES-APP-1).
	ErrHandleFailed ErrorCode = "handle_failed"
	ErrUnsupported  ErrorCode = "unsupported"
)

// Error is the kernel's discriminable error value.
type Error struct {
	Code      ErrorCode
	Operation string
	SessionID SessionID
	CommitID  CommitID
	Detail    string
}

func (e *Error) Error() string {
	s := fmt.Sprintf("session: %s: %s", e.Operation, e.Code)
	if e.SessionID != "" {
		s += fmt.Sprintf(" session=%s", e.SessionID)
	}
	if e.CommitID != "" {
		s += fmt.Sprintf(" commit=%s", e.CommitID)
	}
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	return s
}

// Is lets callers match on the code: errors.Is(err, &Error{Code: ErrNotFound}).
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	if !ok {
		return false
	}
	return t.Code == e.Code && (t.Operation == "" || t.Operation == e.Operation)
}

func newError(code ErrorCode, op string, sid SessionID, detail string) *Error {
	return &Error{Code: code, Operation: op, SessionID: sid, Detail: detail}
}

// IsCode reports whether err is a kernel Error with the given code.
func IsCode(err error, code ErrorCode) bool {
	var e *Error
	for err != nil {
		var ce *Error
		if errors.As(err, &ce) {
			e = ce
			break
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			break
		}
		err = u.Unwrap()
	}
	return e != nil && e.Code == code
}
