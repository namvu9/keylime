package queries

import (
	"reflect"
	"testing"
)

func TestParse(t *testing.T) {
	for i, test := range []struct {
		tokens     []Token
		Collection string
		Command    Command
		Arguments  map[string]string
		Data       map[string]interface{}
	}{
		{
			tokens: []Token{
				{Type: Keyword, Value: "WITH"},
				{Type: StringValue, Value: "{\"age\": \"lol\", \"obj\": {\"number\": [1,2,3]}}"},
				{Type: Keyword, Value: "SET"},
				{Type: Identifier, Value: "1"},
				{Type: Keyword, Value: "IN"},
				{Type: Identifier, Value: "test"},
				{Type: Delimiter, Value: SEMICOLON},
				EOFToken,
			},
			Collection: "test",
			Command:    Set,
			Arguments: map[string]string{
				"key": "1",
			},
			Data: map[string]interface{}{
				"age": "lol",
				"obj": map[string]interface{}{
					"number": []interface{}{1.0, 2.0, 3.0},
				},
			},
		},
		//{
		//tokens: []Token{
		//{Type: "Command", Value: "GET"},
		//{Type: "Argument", Value: "a,b,c,d,e[0],f[0:]"},
		//{Type: "Clause", Value: "FROM"},
		//{Type: "Argument", Value: "a"},
		//{Type: "Clause", Value: "IN"},
		//{Type: "Argument", Value: "test"},
		//{Type: "EOF", Value: "EOF"},
		//},
		//Collection: "test",
		//Command:    Get,
		//Arguments: map[string]string{
		//"key":       "a",
		//"selectors": "a,b,c,d,e[0],f[0:]",
		//},
		//},
		{
			tokens: []Token{
				{Type: Keyword, Value: "GET"},
				{Type: Identifier, Value: "a"},
				{Type: Keyword, Value: "IN"},
				{Type: Identifier, Value: "test"},
				{Type: Delimiter, Value: SEMICOLON},
				EOFToken,
			},
			Collection: "test",
			Command:    Get,
			Arguments: map[string]string{
				"key": "a",
			},
		},
	} {
		p := NewParser("")
		p.tokens = test.tokens
		op, err := p.Parse()

		if err != nil {
			t.Errorf("%d: Unexpected parsing error: %s", i, err)
		}

		if op.Collection != test.Collection {
			t.Errorf("%d: Collection: want=%s got=%s", i, test.Collection, op.Collection)
		}

		if op.Command != test.Command {
			t.Errorf("%d: Command: Want=%s Got=%s", i, test.Command, op.Command)
		}

		if want, got := len(test.Arguments), len(op.Arguments); got != want {
			t.Errorf("len(arguments) want=%d got=%d", want, got)
		}
		for k, v := range test.Arguments {
			if got := op.Arguments[k]; got != v {
				t.Errorf("%d: Arguments[%s]: Want=%s Got=%s", i, k, v, got)
			}
		}

		if want, got := len(test.Data), len(op.Payload.Data); got != want {
			t.Errorf("len(arguments) want=%d got=%d", want, got)
		}
		for k, v := range test.Data {
			if got := op.Payload.Data[k]; !reflect.DeepEqual(got, v) {
				t.Errorf("Data[%s]: Want=%v Got=%v", k, v, got)
			}
		}
	}

}
