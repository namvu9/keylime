package queries

import (
	"testing"
)

func TestTokenizer(t *testing.T) {
	t.Run("Delimiters", func(t *testing.T) {
		input := ".,;:{}()[]?"
		tokens := tokenize(input)

		expTokens := []string{
			PERIOD,
			COMMA,
			SEMICOLON,
			COLON,
			LBRACE,
			RBRACE,
			LPAREN,
			RPAREN,
			LBRACKET,
			RBRACKET,
			QUESTIONMARK,
		}

		if want, got := len(expTokens), len(tokens); want != got {
			t.Errorf("len(tokens) want=%d got=%d", want, got)
		}

		for i, token := range tokens {
			if token.Type != Delimiter {
				t.Errorf("Expected token type Delimiter got=%v", token.Type)
			}
			if token.Value != expTokens[i] {
				t.Errorf("Token want %v got %v", expTokens[i], token)
			}
		}
	})

	t.Run("Keywords", func(t *testing.T) {
		input := "SELECT   SET DELETE UPDATE CREATE SCHEMA WITH IN FROM String Number Array Object Map Boolean"

		tokens := tokenize(input)

		expTokens := []string{
			"SELECT",
			"SET",
			"DELETE",
			"UPDATE",
			"CREATE",
			"SCHEMA",
			"WITH",
			"IN",
			"FROM",
			"String",
			"Number",
			"Array",
			"Object",
			"Map",
			"Boolean",
		}

		if want, got := len(expTokens), len(tokens); want != got {
			t.Errorf("len(tokens) want=%d got=%d", want, got)
		}

		for i, token := range tokens {
			if token.Type != Keyword {
				t.Errorf("Expected token type Keyword got=%v", token.Type)
			}
			if token.Value != expTokens[i] {
				t.Errorf("Token want %v got %v", expTokens[i], token.Value)
			}
		}
	})

	t.Run("Identifiers and Values", func(t *testing.T) {
		input := "afalseprophet false true nam 90490 a980 x lol \"dude\" '\"This\" is a string'"

		tokens := tokenize(input)

		expTokens := []Token{
			{
				Type:  Identifier,
				Value: "afalseprophet",
			},
			{
				Type:  BooleanValue,
				Value: "false",
			},
			{
				Type:  BooleanValue,
				Value: "true",
			},
			{
				Type:  Identifier,
				Value: "nam",
			},
			{
				Type:  NumberValue,
				Value: "90490",
			},
			{
				Type:  Identifier,
				Value: "a980",
			},
			{
				Type:  Identifier,
				Value: "x",
			},
			{
				Type:  Identifier,
				Value: "lol",
			},
			{
				Type:  StringValue,
				Value: "dude",
			},
			{
				Type:  StringValue,
				Value: "\"This\" is a string",
			},
		}

		if want, got := len(expTokens), len(tokens); want != got {
			t.Errorf("len(tokens) want=%d got=%d", want, got)
		}

		for i, token := range tokens {
			if token != expTokens[i] {
				t.Errorf("Token want %v got %v", expTokens[i], token)
			}
		}
	})

	t.Run("Set with data", func(t *testing.T) {
		input := ` WITH '{"age": 4}' SET doc IN testcollection;
	`
		tokens := tokenize(input)

		expTokens := []Token{
			{
				Type:  Keyword,
				Value: "WITH",
			},
			{
				Type:  StringValue,
				Value: "{\"age\": 4}",
			},
			{
				Type:  Keyword,
				Value: "SET",
			},
			{
				Type:  Identifier,
				Value: "doc",
			},
			{
				Type:  Keyword,
				Value: "IN",
			},
			{
				Type:  Identifier,
				Value: "testcollection",
			},
			{
				Type:  Delimiter,
				Value: SEMICOLON,
			},
		}

		if want, got := len(expTokens), len(tokens); want != got {
			t.Errorf("len(tokens) want=%d got=%d", want, got)
		}

		for i, token := range expTokens {
			if token != tokens[i] {
				t.Errorf("Token want %v got %v", tokens[i], token)
			}
		}
	})

	t.Run("Schema", func(t *testing.T) {
		input := `
	{
		age?: Number,
		name: String(0,10),
		longName: []Number,
		map: Map,
		object: {
			age: Number = 4
		}
	}
	`
		tokens := tokenize(input)

		expTokens := []Token{
			{
				Type:  Delimiter,
				Value: LBRACE,
			},
			{
				Type:  Identifier,
				Value: "age",
			},
			{
				Type:  Delimiter,
				Value: QUESTIONMARK,
			},
			{
				Type:  Delimiter,
				Value: COLON,
			},
			{
				Type:  Keyword,
				Value: "Number",
			},
			{
				Type:  Delimiter,
				Value: COMMA,
			},
			{
				Type:  Identifier,
				Value: "name",
			},
			{
				Type:  Delimiter,
				Value: COLON,
			},
			{
				Type:  Keyword,
				Value: "String",
			},
			{
				Type:  Delimiter,
				Value: LPAREN,
			},
			{
				Type:  NumberValue,
				Value: "0",
			},
			{
				Type:  Delimiter,
				Value: COMMA,
			},
			{
				Type:  NumberValue,
				Value: "10",
			},
			{
				Type:  Delimiter,
				Value: RPAREN,
			},
			{
				Type:  Delimiter,
				Value: COMMA,
			},
			{
				Type:  Identifier,
				Value: "longName",
			},
			{
				Type:  Delimiter,
				Value: COLON,
			},
			{
				Type:  Delimiter,
				Value: LBRACKET,
			},
			{
				Type:  Delimiter,
				Value: RBRACKET,
			},
			{
				Type:  Keyword,
				Value: "Number",
			},
			{
				Type:  Delimiter,
				Value: COMMA,
			},
			{
				Type:  Identifier,
				Value: "map",
			},
			{
				Type:  Delimiter,
				Value: COLON,
			},
			{
				Type:  Keyword,
				Value: "Map",
			},
			{
				Type:  Delimiter,
				Value: COMMA,
			},
			{
				Type:  Identifier,
				Value: "object",
			},
			{
				Type:  Delimiter,
				Value: COLON,
			},
			{
				Type:  Delimiter,
				Value: LBRACE,
			},
			{
				Type:  Identifier,
				Value: "age",
			},
			{
				Type:  Delimiter,
				Value: COLON,
			},
			{
				Type:  Keyword,
				Value: "Number",
			},
			{
				Type:  Delimiter,
				Value: EQUALS,
			},
			{
				Type:  NumberValue,
				Value: "4",
			},
			{
				Type:  Delimiter,
				Value: RBRACE,
			},
			{
				Type:  Delimiter,
				Value: RBRACE,
			},
		}

		if want, got := len(expTokens), len(tokens); want != got {
			t.Errorf("len(tokens) want=%d got=%d", want, got)
		}

		for i, token := range expTokens {
			if token != tokens[i] {
				t.Errorf("Token want %v got %v", tokens[i], token)
			}
		}
	})
}
