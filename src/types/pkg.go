// Package types defines the Keylime type system.
//
// The basic types include:
//
// * Boolean
// * Number
// * String
// * Object
// * Array
//
// More complex and abstract data types are built on top of
// these basic types.
//
// The Document type is a set, identified by a unique key,
// of key-value pairs or 'fields'. On its own, a document
// may contain any number of fields whose values be of any
// type. Similarly, Object-type fields may contain any set
// of fields and Array-type fields may contain any type of
// elements, including mixed types.
//
// A `Collection` is a named set of documents. The key that
// identifies a document need only be unique within a
// collection.
//
// A `Schema` may be applied at the collection level to
// define the set of fields a document must contain. The
// schema may also constrain the type of elements an
// Array-type field may contain.
package types

import (
	"context"
	"encoding/gob"
)

func init() {
	gob.Register(map[string]interface{}{})
	gob.Register([]interface{}{})
}

type Store interface {
	Collection(name string) (Collection, error)
}

// A Collection represents a named set of Documents.
type Collection interface {
	Get(ctx context.Context, k string) (Document, error)
	GetFirst(ctx context.Context, n int) ([]Document, error)
	GetLast(ctx context.Context, n int) ([]Document, error)

	Set(ctx context.Context, k string, fields map[string]interface{}) error
	Delete(ctx context.Context, k string) error
	Update(ctx context.Context, k string, fields map[string]interface{}) error
	Create(ctx context.Context, s *Schema) error

	Info(ctx context.Context)
}

type Type string

func (t Type) Is(other Type) bool {
	return t == other
}

// KeyLime data types
const (
	Boolean Type = "Boolean"
	Number       = "Number"
	Object       = "Object" // Object is a Map with a schema
	Map          = "Map"
	Array        = "Array"
	String       = "String"
	Unknown      = "Unknown"
)

func GetDataType(s interface{}) Type {
	switch s.(type) {
	case string:
		return String
	case int, float32, float64, uint:
		return Number
	case map[string]interface{}:
		return Map
	case map[string]Field:
		return Object
	case []interface{}:
		return Array
	case bool:
		return Boolean
	default:
		return Unknown
	}
}

// Identifier is interface wrapping the basic ID method
type Identifier interface {
	ID() string
}
