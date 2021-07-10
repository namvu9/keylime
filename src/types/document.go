package types

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Document represents a set of named fields. They are
// recursively defined and may contain other embedded
// documents.
type Document struct {
	Key          string
	Fields       map[string]Field
	CreatedAt    time.Time
	LastModified time.Time
	Deleted      bool
}

func (d Document) Hash() string {
	s := sha256.New()
	enc := gob.NewEncoder(s)
	
	for _, field := range d.Fields {
		enc.Encode(field.Value)
	}

	return base64.StdEncoding.EncodeToString(s.Sum(nil))
}

// Set the values of one or more fields whose type are
// inferred from the values' concrete type. Optionally, a
// schema may be provided to enforce the types of the
// fields. Note, however, that the schema is not applied in
// future calls to Set. If this is desired, apply the
// schema at the collection level.
// TODO: TEST
func (d Document) Set(fields map[string]interface{}, schemas ...Schema) Document {
	d.Fields = make(map[string]Field)
	for name, value := range fields {
		d.Fields[name] = newField(value)
	}

	d.LastModified = time.Now()

	return d
}

// Update returns a copy of the current document, updated
// with the provided fields.
func (d Document) Update(fields map[string]interface{}) Document {
	c := d.clone()
	for name, value := range fields {
		c.Fields[name] = newField(value)
	}

	return c
}

// TODO: test
func (d Document) Get(fieldPath ...string) (Field, bool) {
	f, ok := d.Fields[fieldPath[0]]
	if !ok {
		return f, false
	}
	if len(fieldPath) > 1 {
		for _, fieldName := range fieldPath[1:] {
			if !f.IsOneOf(Object, Map) {
				return f, false
			}

			ob, ok := f.Value.(map[string]interface{})
			if !ok {
				return f, false
			}

			v, ok := ob[fieldName]
			if !ok {
				return f, false
			}

			f = newField(v)
		}
	}

	return f, true
}

func (d Document) Select(selectFn ...FieldSelector) map[string]interface{} {
	out := make(map[string]interface{})

	for _, selectFn := range selectFn {
		name, field, ok := selectFn(d)
		if !ok {
			out[name] = fmt.Errorf("Value at fieldpath %s does not exist", name)
		} else {
			out[name] = field
		}
	}

	return out
}

func (d Document) String() string {
	s, _ := prettify(d.Fields)
	return fmt.Sprintf("%s=%s", d.Key, s)
}

func (d Document) compare(by compareFunc, other Document) int {
	return by(d, other)
}

func (d Document) clone() Document {
	fields := make(map[string]Field)
	for name, field := range d.Fields {
		fields[name] = field
	}

	d.Fields = fields

	return d
}

func NewDoc(key string) Document {
	return Document{
		Key:          key,
		CreatedAt:    time.Now(),
		LastModified: time.Now(),
		Fields:       make(map[string]Field),
	}
}

type Field struct {
	Type  Type
	Value interface{}
}

// TODO: Test
func (f *Field) ToMap() error {
	var (
		v       = make(map[string]interface{})
		jsonstr = f.Value.(string)
	)

	if err := json.Unmarshal([]byte(jsonstr), &v); err != nil {
		return err
	}

	f.Value = v
	f.Type = Map

	return nil
}

// TODO: Test
func (f *Field) ToObject() error {
	m := make(map[string]interface{})

	if f.IsType(Map) {
		m = f.Value.(map[string]interface{})
	}

	if f.IsType(String) {
		jsonstr := f.Value.(string)
		if err := json.Unmarshal([]byte(jsonstr), &m); err != nil {
			return err
		}
	}

	f.Value = m
	f.Type = Object

	return nil
}

// TODO: Test
func (f *Field) ToNumber() error {
	v, err := strconv.ParseFloat(f.Value.(string), 64)
	if err != nil {
		return err
	}

	f.Value = v
	f.Type = Number

	return nil
}

// TODO: Test
func (f *Field) ToBoolean() error {
	v, err := strconv.ParseBool(f.Value.(string))
	if err != nil {
		return err
	}

	f.Value = v
	f.Type = Boolean
	return nil
}

// TODO: implement
func (f *Field) ToArray() error {
	return nil
}

// TODO: test
func (f *Field) Validate(name string, schemaField SchemaField) []error {
	var (
		errs = []error{}
	)

	if f.IsType(Unknown) {
		errs = append(errs, fmt.Errorf("Field has unknown type: %s", name))
		return errs
	}

	// Test this case
	if f.Type != schemaField.Type {
		err := f.ToType(schemaField.Type)
		if err != nil {
			errs = append(errs, fmt.Errorf("Expected value of type %s but got %s", schemaField.Type, f.Type))
			return errs
		}

		return f.Validate(name, schemaField)
	}

	if f.IsType(Object) {
		obj := f.Value.(map[string]interface{})
		r := NewDoc("k")
		r.Set(obj)
		objErrs := schemaField.Schema.Validate(r)

		if objErrs != nil {
			errs = append(errs, objErrs)
		}
	}

	if f.IsType(Array) {
		arr, ok := f.Value.([]interface{})
		if !ok {
			errs = append(errs, fmt.Errorf("Could not convert value to []interface{}"))
			return errs
		}

		if schemaField.ElementType != nil {
			for _, e := range arr {
				if got := GetDataType(e); got != *schemaField.ElementType {
					errs = append(errs, fmt.Errorf("Expected array element of type %s but got %s", *schemaField.ElementType, got))
				}
			}
		}

		if schemaField.Min != nil && !IsGreaterThan(len(arr), *schemaField.Min) {
			errs = append(errs, fmt.Errorf("Expected at least %d elements, Got %d", *schemaField.Min, len(arr)))
		}

		if schemaField.Max != nil && !IsLessThan(len(arr), *schemaField.Max) {
			errs = append(errs, fmt.Errorf("Expected at most %d elements, Got %d", *schemaField.Max, len(arr)))
		}
	}

	if f.IsType(String) {
		s, ok := f.Value.(string)
		if !ok {
			errs = append(errs, fmt.Errorf("Could not convert field value to string"))
			return errs
		}

		if schemaField.Min != nil && !IsGreaterThan(len(s), *schemaField.Min) {
			errs = append(errs, fmt.Errorf("Expected at least %d elements, Got %d", *schemaField.Min, len(s)))
		}

		if schemaField.Max != nil && !IsLessThan(len(s), *schemaField.Max) {
			errs = append(errs, fmt.Errorf("Expected at most %d elements, Got %d", *schemaField.Max, len(s)))
		}
	}

	if len(errs) != 0 {
		return errs
	}

	return nil
}

func IsWithinRange(n int, min, max int) bool {
	return n >= min && n <= max
}

func IsGreaterThan(n int, min int) bool {
	return n >= min
}

func IsLessThan(n int, max int) bool {
	return n <= max
}

func (f *Field) ToType(t Type) error {
	switch t {
	case Map:
		err := f.ToMap()
		if err != nil {
			return err
		}

	case Object:
		err := f.ToObject()
		if err != nil {
			return err
		}
	case Number:
		if !f.IsType(String) {
			return fmt.Errorf("TypeConversionError: Cannot convert %s to %s", f.Type, t)
		}
		err := f.ToNumber()
		if err != nil {
			return err
		}
	case Boolean:
		if !f.IsType(String) {
			return fmt.Errorf("TypeConversionError: Cannot convert %s to %s", f.Type, t)
		}
		err := f.ToBoolean()
		if err != nil {
			return err
		}

	case Array:
		// TODO: Implmement

	default:
		return fmt.Errorf("TypeConversionError: Cannot convert %s to %s", f.Type, t)
	}

	return nil
}

func (f *Field) IsType(t Type) bool {
	return f.Type == t
}

func (f *Field) IsOneOf(ts ...Type) bool {
	for _, t := range ts {
		if f.Type == t {
			return true
		}
	}
	return false
}

type FieldSelector func(Document) (string, Field, bool)

// TODO: test
func MakeFieldSelectors(selectors ...string) []FieldSelector {
	out := []FieldSelector{}

	for _, selector := range selectors {
		name := selector

		out = append(out, func(r Document) (string, Field, bool) {
			fieldPath := strings.Split(name, ".")
			f, ok := r.Fields[fieldPath[0]]

			if !ok {
				return name, f, false
			}

			if len(fieldPath) > 1 {
				for _, fieldName := range fieldPath[1:] {
					if !f.IsOneOf(Object, Map) {
						return name, f, false
					}

					ob, ok := f.Value.(map[string]interface{})
					if !ok {
						return name, f, false
					}

					v, ok := ob[fieldName]
					if !ok {
						return name, f, false
					}

					f = newField(v)
				}

			}

			return name, f, true
		})

	}

	return out
}

func prettify(v interface{}) (string, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "", err
	}
	return string(b), nil
}

type compareFunc func(Document, Document) int
