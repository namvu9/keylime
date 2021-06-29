package errors

import (
	"fmt"
)

const (
	KeyNotFoundError Kind = iota
	InternalError
	IOError
)

// OP
// A unique string describing a method or a function
// Multiple operations can construct a friendly stack trace

type Op string
type Kind int

type Error struct {
	Op   Op          // Operation (where)
	Kind interface{} // Category
	Err  error       // The wrapped error (why)
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s:\n  %s", e.Op, e.Err.Error())
}

func (e Error) Is(target error) bool {
	if other, ok := target.(*Error); ok {
		return e.Kind == other.Kind
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
		Kind: KeyNotFoundError,
		Err:  fmt.Errorf("KeyNotFound: %s", key),
	}
}

func Wrap(op Op, kind Kind, err error) *Error {
	return &Error{
		Op:   op,
		Kind: kind,
		Err:  err,
	}
}
