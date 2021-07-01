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
			Options []SchemaFieldOption
			Errors  []errors.Code
		}{
			{
				Type:    String,
				Options: []SchemaFieldOption{WithDefault(4)},
				Errors:  []errors.Code{errors.EInternal},
			},
			{
				Type:    Boolean,
				Options: []SchemaFieldOption{WithDefault(4)},
				Errors:  []errors.Code{errors.EInternal},
			},
			{
				Type:    Number,
				Options: []SchemaFieldOption{WithDefault("lol")},
				Errors:  []errors.Code{errors.EInternal},
			},
			{
				Type:    Map,
				Options: []SchemaFieldOption{WithDefault("lol")},
				Errors:  []errors.Code{errors.EInternal},
			},
			{
				Type:    Object,
				Options: []SchemaFieldOption{WithDefault("lol"), WithSchema(&Schema{})},
				Errors:  []errors.Code{errors.EInternal},
			},
		} {
			sb := NewSchemaBuilder()

			sb.AddField("whatever", test.Type, test.Options...)
			_, err := sb.Build()

			if len(err) != len(test.Errors) {
				fmt.Println(err)
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
			Options []SchemaFieldOption
			Errors  []errors.Code
		}{
			{
				Type:    String,
				Options: []SchemaFieldOption{WithDefault("Nam")},
				Errors:  []errors.Code{errors.EInternal},
			},
			{
				Type:    Boolean,
				Options: []SchemaFieldOption{WithDefault(true)},
				Errors:  []errors.Code{errors.EInternal},
			},
			{
				Type:    Number,
				Options: []SchemaFieldOption{WithDefault(30.2)},
				Errors:  []errors.Code{errors.EInternal},
			},
			{
				Type:    Number,
				Options: []SchemaFieldOption{WithDefault(30)},
				Errors:  []errors.Code{errors.EInternal},
			},
			{
				Type:    Object,
				Options: []SchemaFieldOption{WithDefault(map[string]interface{}{}), WithSchema(&Schema{})},
				Errors:  []errors.Code{errors.EInternal},
			},
		} {
			sb := NewSchemaBuilder()

			sb.AddField("whatever", test.Type, test.Options...)
			_, err := sb.Build()

			if len(err) != 0 {
				fmt.Println(err)
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

		r := NewRecord("someKey")
		r.SetFields(test.fields)

		err := schema.Validate(r)

		if len(err) != len(test.invalidFields) {
			t.Errorf("%d: Want %d errors, got %d", i, len(test.invalidFields), len(err))
		}

		for _, fieldName := range test.invalidFields {
			if err[fieldName] == nil {
				t.Errorf("%d: Expected %s (%v) to be invalid", i, fieldName, test.fields["age"])
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
}

func TestImplicitTypeConversion(t *testing.T) {
	for i, test := range []struct {
		input        interface{}
		fieldType    Type
		inferredType Type
		opts         []SchemaFieldOption
	}{
		{
			input:        "4",
			inferredType: String,
			fieldType:    String,
		},
		{
			input:        4,
			inferredType: Number,
			fieldType:    Number,
		},
		{
			input:        4.32,
			inferredType: Number,
			fieldType:    Number,
		},
		{
			input:        "4",
			inferredType: String,
			fieldType:    Number,
		},
		{
			input:        "4.32",
			inferredType: String,
			fieldType:    Number,
		},
		{
			input:        false,
			inferredType: Boolean,
			fieldType:    Boolean,
		},
		{
			input:        "false",
			inferredType: String,
			fieldType:    Boolean,
		},
		{
			input:        true,
			inferredType: Boolean,
			fieldType:    Boolean,
		},
		{
			input:        "true",
			inferredType: String,
			fieldType:    Boolean,
		},
		{
			input:        map[string]interface{}{},
			inferredType: Map,
			fieldType:    Map,
		},
		{
			input:        map[string]interface{}{},
			inferredType: Map,
			fieldType:    Object,
			opts:         []SchemaFieldOption{WithSchema(&Schema{})},
		},
		{
			input:        []interface{}{4},
			inferredType: Array,
			fieldType:    Array,
		},
	} {
		sb := NewSchemaBuilder()
		sb.AddField("Test", test.fieldType, test.opts...)
		schema, err := sb.Build()
		if err != nil {
			t.Logf("Schema build failed: %s", err)
		}

		r := NewRecord("k")
		r.Set("Test", test.input)

		field, _ := r.Get("Test")

		if !field.IsType(test.inferredType) {
			t.Errorf("%d: Inferred type, want %s got %s", i, test.inferredType, field.Type)
		}

		e := schema.Validate(r)
		if e != nil {
			t.Logf("Schema validation fails: %s", err)
		}

		field2, _ := r.Get("Test")

		if !field2.IsType(test.fieldType) {
			t.Errorf("%d: Expected final type to be %s got %s", i, test.fieldType, field.Type)
		}
	}
}

func TestSchemaWithDefaults(t *testing.T) {
	personSchema := &Schema{
		fields: map[string]SchemaField{
			"age": {
				Type:         Number,
				Required:     false,
				DefaultValue: 10,
			},
		},
	}

	def := map[string]interface{}{"age": 4}
	sb := NewSchemaBuilder()
	sb.AddField("name", String, WithDefault("Godzilla"), Optional)
	sb.AddField("age", Number, WithDefault(4))
	sb.AddField("ob", Object, WithSchema(personSchema))
	sb.AddField("map", Map, WithDefault(def), Optional)

	schema, err := sb.Build()
	if err != nil {
		t.Errorf("Did not expect schema build to fail")
	}

	r := NewRecord("k")
	r.Set("name", "Nam")
	rCopy := schema.WithDefaults(r)

	if r == rCopy {
		t.Errorf("Expected schema.WithDefaults to return a copy, but got identical struct")
	}

	def["age"] = 8

	val, ok := rCopy.Get("map")
	if !ok {
		t.Errorf("Expected default value for field 'map'")
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
