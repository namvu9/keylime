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
			"EOF",
		}

		if want, got := len(expTokens), len(tokens); want != got {
			t.Errorf("len(tokens) want=%d got=%d", want, got)
		}

		for i, token := range tokens {
			if token.Type != DelimiterToken && token.Type != "EOF" {
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
			"EOF",
		}

		if want, got := len(expTokens), len(tokens); want != got {
			t.Errorf("len(tokens) want=%d got=%d", want, got)
		}

		for i, token := range tokens {
			if token.Type != KeywordToken && token.Type != EOF {
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
			Identifier("afalseprophet"),
			Boolean("false"),
			Boolean("true"),
			Identifier("nam"),
			Number("90490"),
			Identifier("a980"),
			Identifier("x"),
			Identifier("lol"),
			String("dude"),
			String( "\"This\" is a string"),
			EOFToken,
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
			Keyword("WITH"),
			String("{\"age\": 4}"),
			Keyword("SET"),
			Identifier("doc"),
			Keyword("IN"),
			Identifier("testcollection"),
			Delimiter(SEMICOLON),
			EOFToken,
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
		longName: []Number(,10),
		map: Map,
		object: {
			age: Number = 4
		}
	}
	`
		tokens := tokenize(input)

		expTokens := []Token{
			Delimiter(LBRACE),
			Identifier("age"),
			Delimiter(QUESTIONMARK),
			Delimiter(COLON),
			Keyword("Number"),
			Delimiter(COMMA),
			Identifier("name"),
			Delimiter(COLON),
			Keyword("String"),
			Delimiter(LPAREN),
			Number("0"),
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
			Delimiter(EQUALS),
			Number("4"),
			Delimiter(RBRACE),
			Delimiter(RBRACE),
			EOFToken,
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
