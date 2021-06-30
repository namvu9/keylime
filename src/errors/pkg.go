package errors

import (
	"fmt"
)

// TODO: Rethink this
const (
	KeyNotFoundError Kind = iota
	InternalError
	IOError
	InvalidArguments
	InvalidSchemaError
	SchemaValidationError
	Unknown
)

// OP
// A unique string describing a method or a function
// Multiple operations can construct a friendly stack trace

type Op string
type Kind int

func (k Kind) String() string {
	switch k {
	case KeyNotFoundError:
		return "KeyNotFoundError"
	case InternalError:
		return "InternalError"
	case IOError:
		return "IOError"
	case InvalidArguments:
		return "InvalidArguments"
	case InvalidSchemaError:
		return "InvalidSchemaError"
	default:
		return "Unknown"
	}
}

func GetKind(e interface{}) Kind {
	if v, ok := e.(*Error); ok {
		return v.Kind
	}

	if v, ok := e.(Error); ok {
		return v.Kind
	}

	return Unknown
}

type Error struct {
	Op   Op    // Operation (where)
	Kind Kind  // Category
	Err  error // The wrapped error (why)

	Collection string
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

func WrapWith(op Op, kind Kind) func(error) *Error {
	return func(e error) *Error {
		return &Error{
			Op:   op,
			Kind: kind,
			Err:  e,
		}
	}
}
