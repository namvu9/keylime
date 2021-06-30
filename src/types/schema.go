package types

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strconv"
	"strings"

	"github.com/namvu9/keylime/src/errors"
)

type Schema struct {
	fields map[string]Field
}

func (s *Schema) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(s.fields)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (s *Schema) GobDecode(data []byte) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&s.fields)
	return err
}

type ValidationError map[string][]error

func (ve ValidationError) Error() string {
	var sb strings.Builder
	sb.WriteString("\nInvalid fields:\n")
	for name, err := range ve {
		sb.WriteString(fmt.Sprintf("%s:\n", name))
		for _, e := range err {
			sb.WriteString(fmt.Sprintf("%s\n", e.Error()))
		}
	}
	sb.WriteString("\n")
	return sb.String()
}

func (s *Schema) Validate(r *Record) ValidationError {
	wrapError := errors.WrapWith("(*Schema).Validate", errors.SchemaValidationError)
	es := make(ValidationError)
	if s == nil {
		return es
	}

	for name, data := range r.Data {
		expected, ok := s.fields[name]

		if !ok {
			es[name] = append(es[name], wrapError(fmt.Errorf("Unknown field %s", name)))
		}

		if data.Type == Unknown {
			wrapError(fmt.Errorf("Value has type Unknown"))
			continue
		}

		// Test this case
		if data.Type == String && expected.Type == Number {
			v, err := strconv.ParseFloat(data.Value.(string), 64)
			if err == nil {
				r.Set(name, v)
				continue
			}
		}

		if data.Type != expected.Type {
			es[name] = append(es[name], wrapError(fmt.Errorf("Expected value of type %s but got %s", expected.Type, data.Type)))
		}
	}

	for name, field := range s.fields {
		if _, ok := r.Data[name]; !ok && field.Required {
			es[name] = append(es[name], wrapError(fmt.Errorf("Required field missing: %s", name)))
		}
	}

	if len(es) == 0 {
		return nil
	}

	return es
}

func (s *Schema) String() string {
	var sb strings.Builder
	sb.WriteString("SCHEMA:")

	for name, field := range s.fields {
		sb.WriteString(fmt.Sprintf("\n- %s\n", name))
		sb.WriteString(fmt.Sprintf("* Type: %s\n", field.Type))
		sb.WriteString(fmt.Sprintf("* Required: %v\n", field.Required))
		if field.Default != nil {
			sb.WriteString(fmt.Sprintf("* Default: %v\n", field.Default))
		}

	}

	return sb.String()
}

type SchemaBuilder struct {
	fields map[string]Field
}

func (s *SchemaBuilder) Build() (*Schema, []error) {
	wrapError := errors.WrapWith("(*SchemaBuilder).Build", errors.InvalidSchemaError)

	schema := NewSchema()
	errors := []error{}

	for name, field := range s.fields {
		// Check whether the default value can be converted to
		// the specified type
		if field.Default != nil {
			if got := GetDataType(field.Default); got != field.Type {
				errors = append(errors, wrapError(fmt.Errorf("Invalid default value for field of type %s: %v", field.Type, got)))
			}
		}

		schema.fields[name] = field
	}

	if len(errors) > 0 {
		return nil, errors
	}

	return schema, nil
}

// With default makes a copy of Record r's fields and
// and returns a pointer to a record with the schema's
// default values applied
func (s *Schema) WithDefaults(r *Record) *Record {
	rCopy := NewRecord(r.Key)

	// TODO: Does this copy work as intended??
	for name, field := range r.Data {
		rCopy.Data[name] = field
	}

	for name, field := range s.fields {
		_, ok := rCopy.Data[name]
		if field.Default != nil && !ok {
			if field.Type == Object {
				obj := field.Default.(map[string]interface{})
				rCopy.Data[name] = Data{
					Type:  field.Type,
					Value: CopyObj(obj),
				}

			} else {
				rCopy.Data[name] = Data{
					Type:  field.Type,
					Value: field.Default,
				}
			}
		}
	}

	return rCopy
}

func CopyObj(obj map[string]interface{}) map[string]interface{} {
	clone := make(map[string]interface{})

	for key, value := range obj {
		clone[key] = value
	}

	return clone
}

func NewSchema() *Schema {
	return &Schema{
		fields: make(map[string]Field),
	}
}

type FieldOption func(*Field)

// WithDefault sets the default value of a field. This value
// is only applied in cases where the field is optional and
// records that existed before a required field was added
func WithDefault(val interface{}) FieldOption {
	return func(f *Field) {
		f.Default = val
	}
}

func Optional(f *Field) {
	f.Required = false
}

func (s *SchemaBuilder) AddField(name string, valueType Type, opts ...FieldOption) *SchemaBuilder {
	f := Field{
		Type:     valueType,
		Required: true,
	}

	for _, withOpt := range opts {
		withOpt(&f)
	}

	s.fields[name] = f

	return s
}

func NewSchemaBuilder() *SchemaBuilder {
	return &SchemaBuilder{
		fields: make(map[string]Field),
	}
}

type Field struct {
	Type     Type
	Required bool
	Default  interface{} // Old records after extending schema and optional fields
}
type Type int

const (
	Boolean Type = iota
	Number
	Object
	String
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
		return Object
	case bool:
		return Boolean
	default:
		return Unknown
	}
}

type Data struct {
	Type  Type
	Value interface{}
}
