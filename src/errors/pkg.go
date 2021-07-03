package errors

import (
	"fmt"
)

type Code string

// Application error codes
const (
	ENotFound Code = "NotFound"
	EIO            = "IO Error"

	// The application received a request that it did not know
	// how to handle
	EBadRequest = "Bad request"
	EInternal   = "Internal Error"
	EUnknown
)

// OP
// A unique string describing a method or a function
// Multiple operations can construct a friendly stack trace
type Op string

func GetKind(e interface{}) Code {
	if v, ok := e.(*Error); ok {
		return v.Code
	}

	if v, ok := e.(Error); ok {
		return v.Code
	}

	return EUnknown
}

type Error struct {
	Op    Op     // Operation
	Code  Code   // Category
	Err   error  // The wrapped error
	ReqID string // The ID for the current request

	Collection string
}

func GetRequestID(e Error) string {
	return e.ReqID
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s:\n %s", e.Op, e.Err.Error())
}

func (e Error) Is(target error) bool {
	if other, ok := target.(*Error); ok {
		return e.Code == other.Code
	}

	return false
}

// Ops returns the "stack" of operations for an error
func Ops(e *Error) []Op {
	res := []Op{e.Op}

	subErr, ok := e.Err.(*Error)
	if !ok {
		return res
	}

	res = append(res, Ops(subErr)...)
	return res

}

func NewKeyNotFoundError(op Op, key string) *Error {
	return &Error{
		Op:   op,
		Code: ENotFound,
		Err:  fmt.Errorf("KeyNotFound: %s", key),
	}
}

func Wrap(op Op, kind Code, err error) *Error {
	return &Error{
		Op:   op,
		Code: kind,
		Err:  err,
	}
}

func WrapWith(op Op, kind Code) func(error) *Error {
	return func(e error) *Error {
		return &Error{
			Op:   op,
			Code: kind,
			Err:  e,
		}
	}
}
