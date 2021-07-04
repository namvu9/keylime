package queries

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/namvu9/keylime/src/types"
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
				Keyword("WITH"),
				String("{\"age\": \"lol\", \"obj\": {\"number\": [1,2,3]}}"),
				Keyword("SET"),
				Identifier("1"),
				Keyword("IN"),
				Identifier("test"),
				Delimiter(SEMICOLON),
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
		{
			tokens: []Token{
				Keyword("FROM"),
				Identifier("a"),
				Keyword("GET"),
				Identifier("a b c d e[0] f[0:]"),
				Keyword("IN"),
				Identifier("test"),
				EOFToken,
			},
			Collection: "test",
			Command:    Get,
			Arguments: map[string]string{
				"key":       "a",
				"selectors": "a b c d e[0] f[0:]",
			},
		},
		{
			tokens: []Token{
				{Type: KeywordToken, Value: "GET"},
				{Type: IdentifierToken, Value: "a"},
				{Type: KeywordToken, Value: "IN"},
				{Type: IdentifierToken, Value: "test"},
				{Type: DelimiterToken, Value: SEMICOLON},
				EOFToken,
			},
			Collection: "test",
			Command:    Get,
			Arguments: map[string]string{
				"key": "a",
			},
		},
	} {
		op, err := parseTokens(test.tokens)

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

func TestParseSchema(t *testing.T) {
	input := []Token{
		Keyword("WITH"),
		Keyword("SCHEMA"),
		Delimiter(LBRACE),
		Identifier("age"),
		Delimiter(COLON),
		Keyword("Number"),
		Delimiter(QUESTIONMARK),
		Delimiter(COMMA),
		Identifier("name"),
		Delimiter(COLON),
		Keyword("String"),
		Delimiter(LPAREN),
		Number("1"),
		Delimiter(COMMA),
		Number("10"),
		Delimiter(RPAREN),
		Delimiter(COMMA),
		Identifier("longName"),
		Delimiter(COLON),
		Delimiter(LBRACKET),
		Delimiter(RBRACKET),
		Keyword("Number"),
		Delimiter(LPAREN),
		Delimiter(COMMA),
		Number("10"),
		Delimiter(RPAREN),
		Delimiter(COMMA),
		Identifier("map"),
		Delimiter(COLON),
		Keyword("Map"),
		Delimiter(COMMA),
		Identifier("object"),
		Delimiter(COLON),
		Delimiter(LBRACE),
		Identifier("age"),
		Delimiter(COLON),
		Keyword("Number"),
		Delimiter(QUESTIONMARK),
		Delimiter(EQUALS),
		Number("4"),
		Delimiter(RBRACE),
		Identifier("aja"),
		Delimiter(COLON),
		Delimiter(LBRACKET),
		Delimiter(RBRACKET),
		Delimiter(LBRACE),
		Identifier("dufus"),
		Delimiter(COLON),
		Keyword("String"),
		Delimiter(RBRACE),
		Delimiter(RBRACE),
	}

	op, err := parseTokens(input)
	if err != nil {
		t.Error(err)
	}

	schema, ok := op.Payload.Data["schema"]
	if !ok {
		t.Errorf("Did not get schema")
	}

	s, ok := schema.(*types.Schema)
	if !ok {
		t.Errorf("NO SCHEMA")
	}

	err = s.Validate(types.NewDoc("k").Set(map[string]interface{}{
		"name":     "asdf",
		"longName": []interface{}{4},
		"map":      map[string]interface{}{},
		"object":   map[string]interface{}{},
		"aja":      []interface{}{},
	}))

	if err != nil {
		fmt.Println(s)
		t.Error(err)
	}

}
