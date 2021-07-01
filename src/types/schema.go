package types

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strings"

	"github.com/namvu9/keylime/src/errors"
)

type Schema struct {
	fields map[string]SchemaField
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
	var (
		buf = bytes.NewBuffer(data)
		dec = gob.NewDecoder(buf)
	)

	return dec.Decode(&s.fields)
}

// Validate a record's field against the current schema. If
// a field's value is of type String and does not match the
// expected type, an attempt will be made to implicitly
// convert the field's value to the expected type. If
// validation passes, nil is returned.
func (s *Schema) Validate(r *Record) ValidationError {
	var (
		wrapError = errors.WrapWith("(*Schema).Validate", errors.SchemaValidationError)
		es        = make(ValidationError)
	)

	if s == nil {
		return nil
	}

	for name, field := range r.Fields {
		sf, ok := s.fields[name]
		if !ok {
			es[name] = append(es[name], fmt.Errorf("Unknown field: %s", name))
		} else {
			err := field.Validate(name, sf)
			if err != nil {
				es[name] = err
			} else {
				r.Fields[name] = field
			}
		}
	}

	for name, field := range s.fields {
		if v, ok := r.Fields[name]; !ok && field.Required {
			err := wrapError(fmt.Errorf("Required field missing: %s", name))
			es[name] = append(es[name], err)
		} else if v.Value == nil && field.Required {
			err := wrapError(fmt.Errorf("Required field is non-nullable: %s", name))
			es[name] = append(es[name], err)
		}
	}

	if len(es) == 0 {
		return nil
	}

	return es
}

// With default makes a copy of Record r's fields and
// and returns a pointer to a record with the schema's
// default values applied
func (s *Schema) WithDefaults(r *Record) *Record {
	recordClone := r.Clone()

	for name, schemaField := range s.fields {
		_, ok := recordClone.Fields[name]
		if schemaField.DefaultValue != nil && !ok {
			schemaField.ApplyDefault(name, recordClone)
		}
	}

	return recordClone
}

func (s *Schema) String() string {
	var sb strings.Builder
	sb.WriteString("SCHEMA:")

	for name, field := range s.fields {
		sb.WriteString(fmt.Sprintf("\n- %s\n", name))
		sb.WriteString(fmt.Sprintf("* Type: %s\n", field.Type))
		sb.WriteString(fmt.Sprintf("* Required: %v\n", field.Required))
		if field.DefaultValue != nil {
			sb.WriteString(fmt.Sprintf("* Default: %v\n", field.DefaultValue))
		}

	}

	return sb.String()
}

type SchemaBuilder struct {
	fields map[string]SchemaField
}

func (s *SchemaBuilder) AddField(name string, valueType Type, opts ...SchemaFieldOption) *SchemaBuilder {
	f := SchemaField{
		Type:     valueType,
		Required: true,
	}

	for _, withOpt := range opts {
		withOpt(&f)
	}

	s.fields[name] = f

	return s
}

func NewField(v interface{}) Field {
	return Field{
		Type:  GetDataType(v),
		Value: v,
	}
}

// TODO: Return error instead of []error
func (s *SchemaBuilder) Build() (*Schema, []error) {
	wrapError := errors.WrapWith("(*SchemaBuilder).Build", errors.InvalidSchemaError)

	schema := NewSchema()
	errors := []error{}

	for name, schemaField := range s.fields {
		if schemaField.DefaultValue != nil {
			defaultField := NewField(schemaField.DefaultValue)
			defaultType := GetDataType(schemaField.DefaultValue)

			if defaultType.Is(Map) && schemaField.Type.Is(Object) {
				err := defaultField.Validate(name, schemaField)
				errors = append(errors, err...)
			} else if defaultType != schemaField.Type {
				errors = append(errors, wrapError(fmt.Errorf("Invalid default value for field of type %s: %v", schemaField.Type, defaultType)))
			}
		}

		// TODO: TEST
		if schemaField.Type.Is(Object) && schemaField.Schema == nil {
			errors = append(errors, wrapError(fmt.Errorf("Field of type Object must have a schema")))
		}

		schema.fields[name] = schemaField
	}

	if len(errors) > 0 {
		return nil, errors
	}

	return schema, nil
}

func NewSchemaBuilder() *SchemaBuilder {
	return &SchemaBuilder{
		fields: make(map[string]SchemaField),
	}
}

func ExtendSchema(s *Schema) *SchemaBuilder {
	sb := NewSchemaBuilder()
	for name, field := range s.fields {
		sb.fields[name] = field
	}

	return sb
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
		fields: make(map[string]SchemaField),
	}
}

type SchemaFieldOption func(*SchemaField)

// WithDefault sets the default value of a field. This value
// is only applied in cases where the field is optional and
// records that existed before a required field was added
func WithDefault(val interface{}) SchemaFieldOption {
	return func(f *SchemaField) {
		switch v := val.(type) {
		case map[string]interface{}:
			f.DefaultValue = CopyObj(v)
		default:
			f.DefaultValue = val
		}
	}
}

// WithSchema applies a schema to a field of type Object
func WithSchema(s *Schema) SchemaFieldOption {
	return func(sf *SchemaField) {
		sf.Schema = s
	}
}

// WithElementType constrains the element type for a field
// of type Array
func WithElementType(t Type) SchemaFieldOption {
	return func(sf *SchemaField) {
		sf.ElementType = &t
	}
}

// WithRange sets a min and max value for a given field. For
// fields of type String, it sets constraints on the length
// of the string. For fields of type Array it constrains the
// number of elements within the array, and for fields of
// type Number it sets the minimum and maximum number. It
// has no effect on fields of type Object.
func WithRange(min, max int) SchemaFieldOption {
	return func(sf *SchemaField) {
		sf.Min = &min
		sf.Max = &max
	}
}

func WithMax(max int) SchemaFieldOption {
	return func(sf *SchemaField) {
		sf.Max = &max
	}
}

func WithMin(min int) SchemaFieldOption {
	return func(sf *SchemaField) {
		sf.Min = &min
	}
}

func Optional(f *SchemaField) {
	f.Required = false
}

type SchemaField struct {
	Type     Type
	Required bool

	// Optional attributes
	DefaultValue interface{} // Old records after extending schema and optional fields
	Schema       *Schema     // If field is of type Object, it may optionally have a schema
	Min          *int
	Max          *int
	ElementType  *Type
}

func (sf SchemaField) ApplyDefault(name string, r *Record) {
	r.Fields[name] = Field{
		Type:  sf.Type,
		Value: sf.Default(),
	}
}

func (sf SchemaField) Default() interface{} {
	if sf.Type.Is(Object) || sf.Type.Is(Map) {
		obj := sf.DefaultValue.(map[string]interface{})
		return CopyObj(obj)
	}

	return sf.DefaultValue
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
