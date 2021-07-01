package types

import (
	"encoding/gob"
)

func init() {
	gob.Register(map[string]interface{}{})
	gob.Register([]interface{}{})
}

type Type int

func (t Type) Is(other Type) bool {
	return t == other
}

// KeyLime data types
const (
	Boolean Type = iota
	Number
	Object // Object is a Map with a schema
	Map
	String
	Array
	Unknown
)

func (dt Type) String() string {
	switch dt {
	case String:
		return "String"
	case Number:
		return "Number"
	case Object:
		return "Object"
	case Map:
		return "Map"
	case Array:
		return "Array"
	case Boolean:
		return "Boolean"
	default:
		return "Unknown"
	}
}

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

