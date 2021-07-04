package types

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strings"
)

// Schema represents a set of constraints
type Schema struct {
	fields map[string]SchemaField
}

// Validate a document against the current schema. An error
// is returned if validation fails.
func (s *Schema) Validate(doc Document) error {
	var ve = make(ValidationError)

	if s == nil {
		return nil
	}

	// validate the fields in doc
	for _, name := range s.RevComplement(doc) {
		ve[name] = append(ve[name], fmt.Errorf("Unknown field: %s", name))
	}

	for _, name := range s.Intersection(doc) {
		field := doc.Fields[name]
		schemaField := s.fields[name]

		err := field.Validate(name, schemaField)
		if err != nil {
			ve[name] = err
		} else {
			doc.Fields[name] = field
		}
	}

	// Check for missing fields
	for _, name := range s.Complement(doc) {
		schemaField := s.fields[name]
		if schemaField.Required {
			ve[name] = append(ve[name], fmt.Errorf("Required field %s missing", name))
		}
	}

	if len(ve) == 0 {
		return nil
	}

	return ve
}

// RevComplement returns the set of fields present in the
// document but not in the schema
func (s *Schema) RevComplement(doc Document) []string {
	out := []string{}
	for name := range doc.Fields {
		if _, ok := s.fields[name]; !ok {
			out = append(out, name)
		}
	}

	return out

}

// Intersection return the set of fields present in both the
// schema and the document
func (s *Schema) Intersection(doc Document) []string {
	out := []string{}

	for name := range s.fields {
		if field, ok := doc.Fields[name]; ok && field.Value != nil {
			out = append(out, name)
		}
	}

	return out
}

// Complement returns the set of fields present in the
// schema but not in the document
func (s *Schema) Complement(doc Document) []string {
	out := []string{}

	for name := range s.fields {
		if field, ok := doc.Fields[name]; !ok || field.Value == nil {
			out = append(out, name)
		}
	}

	return out
}

// WithDefaults makes a copy of Record r's fields and
// and returns a pointer to a record with the schema's
// default values applied
func (s *Schema) WithDefaults(r Document) Document {
	recordClone := r.clone()

	for name, schemaField := range s.fields {
		_, ok := recordClone.Fields[name]
		if schemaField.DefaultValue != nil && !ok {
			recordClone = schemaField.ApplyDefault(name, recordClone)
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

		if field.Schema != nil {
			sb.WriteString(fmt.Sprintf("* Schema: %v\n", field.Schema))
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

func newField(v interface{}) Field {
	return Field{
		Type:  GetDataType(v),
		Value: v,
	}
}

// Build builds, validates, and returns the schema
func (s *SchemaBuilder) Build() (*Schema, ValidationError) {
	schema := NewSchema()
	errors := make(ValidationError)

	for name, schemaField := range s.fields {
		if schemaField.HasDefault() {
			defaultField := newField(schemaField.DefaultValue)

			if defaultField.IsOneOf(Map, Object) && schemaField.Type.Is(Object) {
				if err := defaultField.Validate(name, schemaField); err != nil {
					errors[name] = err
				}
			} else if !defaultField.IsType(schemaField.Type) {
				errors[name] = append(errors[name], fmt.Errorf("Invalid default value for field of type %s: %v", schemaField.Type, defaultField.Type))
			}
		}

		// TODO: TEST
		if schemaField.Type.Is(Object) && schemaField.Schema == nil {
			errors[name] = append(errors[name], fmt.Errorf("Field of type Object must have a schema"))
		}

		schema.fields[name] = schemaField
	}

	if len(errors) > 0 {
		return nil, errors
	}

	return schema, nil
}

// NewSchemaBuilder instantiates a new SchemaBuilder
func NewSchemaBuilder() *SchemaBuilder {
	return &SchemaBuilder{
		fields: make(map[string]SchemaField),
	}
}

// Extend returns a `SchemaBuilder` that uses the current
// schema as basis.
func (s *Schema) Extend() *SchemaBuilder {
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

// HasDefault reports whether the schema field has a default
// value set
func (sf SchemaField) HasDefault() bool {
	return sf.DefaultValue != nil
}

func (sf SchemaField) ApplyDefault(name string, r Document) Document {
	c := r.clone()
	c.Fields[name] = newField(sf.Default())
	return c
}

func (sf SchemaField) Default() interface{} {
	if sf.Type.Is(Object) || sf.Type.Is(Map) {
		obj := sf.DefaultValue.(map[string]interface{})
		return CopyObj(obj)
	}

	return sf.DefaultValue
}

type ValidationError map[string]FieldValidationError

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

type FieldValidationError []error

func (fve FieldValidationError) Error() string {
	var sb strings.Builder
	for _, e := range fve {
		sb.WriteString(fmt.Sprintf("* %s\n", e))
	}
	return sb.String()
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
