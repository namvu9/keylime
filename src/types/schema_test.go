package types

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/namvu9/keylime/src/errors"
)

func TestSchemaBuilder(t *testing.T) {
	t.Run("Field is required by default", func(t *testing.T) {
		sb := NewSchemaBuilder()
		sb.AddField("username", String)

		schema, err := sb.Build()

		if err != nil {
			t.Errorf("Want=nil, Got=%v", err)
		}

		fieldUserName := schema.fields["username"]
		if !fieldUserName.Required {
			t.Errorf("%s: Fields should be required by default", "username")
		}
	})

	t.Run("Optional Field", func(t *testing.T) {
		sb := NewSchemaBuilder()
		sb.AddField("Whatever", String, Optional)

		schema, _ := sb.Build()
		if schema.fields["Whatever"].Required {
			t.Errorf("Field 'Whatever' should not be required")
		}
	})

	t.Run("InvalidDefaultValue", func(t *testing.T) {
		for i, test := range []struct {
			Type    Type
			Options []FieldOption
			Errors  []errors.Kind
		}{
			{
				Type:    String,
				Options: []FieldOption{WithDefault(4)},
				Errors:  []errors.Kind{errors.InvalidSchemaError},
			},
			{
				Type:    Boolean,
				Options: []FieldOption{WithDefault(4)},
				Errors:  []errors.Kind{errors.InvalidSchemaError},
			},
			{
				Type:    Number,
				Options: []FieldOption{WithDefault("lol")},
				Errors:  []errors.Kind{errors.InvalidSchemaError},
			},
			{
				Type:    Object,
				Options: []FieldOption{WithDefault("lol")},
				Errors:  []errors.Kind{errors.InvalidSchemaError},
			},
		} {
			sb := NewSchemaBuilder()

			sb.AddField("whatever", test.Type, test.Options...)
			_, err := sb.Build()

			if len(err) != len(test.Errors) {
				t.Errorf("%d: Expected %d error, Got %d", i, len(test.Errors), len(err))
			}

			for i, e := range test.Errors {
				if kind := errors.GetKind(err[i]); kind != e {
					fmt.Println(err)
					t.Errorf("Want error kind %s, got %s", e, kind)
				}
			}
		}

	})

	t.Run("Valid default type", func(t *testing.T) {
		for i, test := range []struct {
			Type    Type
			Options []FieldOption
			Errors  []errors.Kind
		}{
			{
				Type:    String,
				Options: []FieldOption{WithDefault("Nam")},
				Errors:  []errors.Kind{errors.InvalidSchemaError},
			},
			{
				Type:    Boolean,
				Options: []FieldOption{WithDefault(true)},
				Errors:  []errors.Kind{errors.InvalidSchemaError},
			},
			{
				Type:    Number,
				Options: []FieldOption{WithDefault(30.2)},
				Errors:  []errors.Kind{errors.InvalidSchemaError},
			},
			{
				Type:    Number,
				Options: []FieldOption{WithDefault(30)},
				Errors:  []errors.Kind{errors.InvalidSchemaError},
			},
			{
				Type:    Object,
				Options: []FieldOption{WithDefault(map[string]interface{}{})},
				Errors:  []errors.Kind{errors.InvalidSchemaError},
			},
		} {
			sb := NewSchemaBuilder()

			sb.AddField("whatever", test.Type, test.Options...)
			_, err := sb.Build()

			if len(err) != 0 {
				t.Errorf("%d: Expected 0 errors, Got %d", i, len(err))
			}
		}
	})
}

func TestSchemaValidation(t *testing.T) {
	sb := NewSchemaBuilder()
	sb.AddField("name", String)
	sb.AddField("age", Number)
	sb.AddField("married", Boolean, WithDefault(false), Optional)

	schema, _ := sb.Build()

	for i, test := range []struct {
		fields        map[string]interface{}
		invalidFields []string
	}{
		{
			fields: map[string]interface{}{
				"age":  4,
				"name": "Nam",
			},
			invalidFields: []string{},
		},
		{
			fields: map[string]interface{}{
				"age":  nil,
				"name": 4,
				"lol":  false,
			},
			invalidFields: []string{"age", "name", "lol"},
		},
		{
			fields: map[string]interface{}{
				"age": 4,
			},
			invalidFields: []string{"name"},
		},
	} {

		r := NewRecord("someKey", nil)
		r.SetFields(test.fields)

		err := schema.Validate(r)

		if len(err) != len(test.invalidFields) {
			t.Errorf("%d: Want %d errors, got %d", i, len(test.invalidFields), len(err))
		}

		for _, fieldName := range test.invalidFields {
			if err[fieldName] == nil {
				t.Errorf("Expected %s to be invalid", fieldName)
			}
		}

		for name := range err {
			var match bool
			for _, fieldName := range test.invalidFields {
				if name == fieldName {
					match = true
				}
			}

			if !match {
				t.Errorf("Unexpected invalid field %s", name)
			}
		}

	}

	// Test valid records
	// Test invalid records
	// Test whether the schema applies the correct defaults
}

func TestSchemaWithDefaults(t *testing.T) {
	def := map[string]interface{}{"age": 4}
	sb := NewSchemaBuilder()
	sb.AddField("name", String, WithDefault("Godzilla"), Optional)
	sb.AddField("ob", Object, WithDefault(def), Optional)

	schema, err := sb.Build()
	if err != nil {
		t.Errorf("Did not expect schema build to fail")
	}

	r := NewRecord("k", nil)
	r.Set("name", "Nam")
	rCopy := schema.WithDefaults(r)

	if r == rCopy {
		t.Errorf("Expected schema.WithDefaults to return a copy, but got identical struct")
	}

	def["age"] = 8

	val, ok := rCopy.Get("ob")

	if !ok {
		t.Errorf("Expected default value for field 'ob'")
	}

	if equal(val.Value, def) {
		t.Errorf("The schema should provide a copy of the default value")
	}

	// Shouldn't override values that have already been set
	val, _ = rCopy.Get("name")
	name := val.Value.(string)
	if name != "Nam" {
		t.Errorf("Want 'Nam', Got %s", name)
	}
}

func equal(a, b interface{}) bool {
	return reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer()
}
