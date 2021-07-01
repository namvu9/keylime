package types

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func Prettify(v interface{}) (string, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "", err
	}
	return string(b), nil
}

type Record struct {
	Key     string
	Value   []byte // Deprecated
	Fields  map[string]Field
	TS      time.Time
	Deleted bool
}

type compareFunc func(Record, Record) int

// TODO: TEST
func (r *Record) Set(name string, value interface{}) {
	r.Fields[name] = NewField(value)
}

func (r *Record) Get(fieldPath ...string) (Field, bool) {
	f, ok := r.Fields[fieldPath[0]]
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

			f = NewField(v)
		}
	}

	return f, true
}

func (r Record) Select(selectFn ...FieldSelector) map[string]interface{} {
	out := make(map[string]interface{})

	for _, selectFn := range selectFn {
		name, field, ok := selectFn(r)
		if !ok {
			out[name] = fmt.Errorf("Value at fieldpath %s does not exist", name)
		} else {
			out[name] = field
		}
	}

	return out
}

func (r Record) CreatedAt() time.Time {
	return r.TS
}

func (r *Record) IsLessThan(other Record) bool {
	return r.Compare(byKey, other) < 0
}

func (r Record) Compare(by compareFunc, other Record) int {
	return by(r, other)
}

func (r Record) IsEqualTo(other *Record) bool {
	return r.Key == other.Key
}

func (r *Record) String() string {
	s, _ := Prettify(r.Fields)
	return fmt.Sprintf("%s=%s", r.Key, s)
}

func (r *Record) SetFields(fields map[string]interface{}) {
	r.Fields = make(map[string]Field)

	for name, value := range fields {
		r.Set(name, value)
	}
}

func (r *Record) Clone() *Record {
	clone := NewRecord(r.Key)
	for name, field := range r.Fields {
		clone.Fields[name] = field
	}

	return clone
}

// TODO: Make sure original isn't affected
func (r *Record) UpdateFields(fields map[string]interface{}) *Record {
	c := r.Clone()

	for name, value := range fields {
		c.Fields[name] = Field{
			Type:  GetDataType(value),
			Value: value,
		}
	}

	return c
}

func byKey(this, that Record) int {
	return strings.Compare(this.Key, that.Key)
}

// TODO: Deprecate
func New(key string, value []byte) Record {
	return Record{
		Key:    key,
		Value:  value,
		TS:     time.Now(),
		Fields: make(map[string]Field),
	}
}

func NewRecord(key string) *Record {
	return &Record{
		Key:    key,
		TS:     time.Now(),
		Fields: make(map[string]Field),
	}
}

type Field struct {
	Type  Type
	Value interface{}
}

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

func (f *Field) ToNumber() error {
	v, err := strconv.ParseFloat(f.Value.(string), 64)
	if err != nil {
		return err
	}

	f.Value = v
	f.Type = Number

	return nil
}

func (f *Field) ToBoolean() error {
	v, err := strconv.ParseBool(f.Value.(string))
	if err != nil {
		return err
	}

	f.Value = v
	f.Type = Boolean
	return nil
}

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
		r := NewRecord("k")
		r.SetFields(obj)
		objErrs := schemaField.Schema.Validate(r)

		if len(objErrs) != 0 {
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
					errs = append(errs, fmt.Errorf("Expected element of type %s but got %s", schemaField.ElementType, got))
				}
			}
		}

		if schemaField.Min != nil {
			if len(arr) < *schemaField.Min {
				errs = append(errs, fmt.Errorf("Expected at least %d elements, Got %d", *schemaField.Min, len(arr)))
			}
		}

		if schemaField.Max != nil {
			if len(arr) < *schemaField.Min {
				errs = append(errs, fmt.Errorf("Expected at most %d elements, Got %d", *schemaField.Max, len(arr)))
			}
		}
	}

	if len(errs) != 0 {
		return errs
	}

	return nil
}

func (f *Field) ToType(t Type) error {
	switch t {
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

type FieldSelector func(Record) (string, Field, bool)

func MakeFieldSelectors(selectors ...string) []FieldSelector {
	out := []FieldSelector{}

	for _, selector := range selectors {
		name := selector
		
		out = append(out, func(r Record) (string, Field, bool) {
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

					f = NewField(v)
				}

			}

			return name, f, true
		})

	}

	return out
}
