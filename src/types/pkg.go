package types

import (
	"encoding/gob"
)

func init() {
	gob.Register(map[string]interface{}{})
	gob.Register([]interface{}{})
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
