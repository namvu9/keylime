package queries

import (
	"fmt"
	"reflect"
	"testing"
)

func TestParse(t *testing.T) {
	for i, test := range []struct {
		input      string
		Collection string
		Command    Command
		Arguments  map[string]string
		Data       map[string]interface{}
	}{
		{
			input:      "WITH {\"age\": \"lol\"} SET 1 IN test",
			Collection: "test",
			Command:    Set,
			Arguments: map[string]string{
				"key": "1",
			},
			Data: map[string]interface{}{
				"age": "lol",
			},
		},
	} {
		p := NewParser(test.input)
		op, err := p.Parse()

		if err != nil {
			t.Errorf("%d: Unexpected parsing error: %s", i, err)
		}

		if op.Collection != test.Collection {
			t.Errorf("%d: Collection: want=%s got=%s", i, test.Collection, op.Collection)
		}

		if op.Command != test.Command {
			t.Errorf("Command: Want=%s Got=%s", test.Command, op.Command)
		}

		for k, v := range test.Arguments {
			if got := op.Arguments[k]; got != v {
				t.Errorf("Arguments[%s]: Want=%s Got=%s", k, v, got)
			}
		}

		for k, v := range test.Data {
			if got := op.Payload.Data[k]; got != v {
				fmt.Println(reflect.TypeOf(got), reflect.TypeOf(v))
				t.Errorf("Data[%s]: Want=%v Got=%v", k, v, got)
			}
		}
	}

}
