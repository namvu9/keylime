package queries

import (
	"encoding/json"
	"strings"
)

type TokenType string

const (
	DelimiterToken  TokenType = "Delimiter"
	KeywordToken              = "Keyword"
	IdentifierToken           = "Identifier"
	StringValue               = "String"
	BooleanValue              = "Boolean"
	NumberValue               = "Number"
	ArrayValue                = "Array"
	ObjectValue               = "Object"
	MapValue                  = "Map"
	EOF                       = "EOF"
)

func Boolean(v string) Token {
	return Token{
		Type:  BooleanValue,
		Value: v,
	}
}

func Number(v string) Token {
	return Token{
		Type:  NumberValue,
		Value: v,
	}
}

func Delimiter(v string) Token {
	return Token{
		Type:  DelimiterToken,
		Value: v,
	}
}

func Keyword(v string) Token {
	return Token{
		Type:  KeywordToken,
		Value: v,
	}
}

func Identifier(v string) Token {
	return Token{
		Type:  IdentifierToken,
		Value: v,
	}
}

func String(v string) Token {
	return Token{
		Type:  StringValue,
		Value: v,
	}
}

func (t Token) IsValueType() bool {
	switch t.Type {
	case StringValue, BooleanValue, NumberValue, ArrayValue, ObjectValue, MapValue:
		return true
	default:
		return false
	}
}

func (t Token) IsDataType() bool {
	if t.Type != KeywordToken {
		return false
	}

	switch t.Value {
	// Use c onstants
	case "String", "Boolean", "Number", "Array", "Object", "Map":
		return true
	default:
		return false
	}
}

var EOFToken = Token{
	Type:  EOF,
	Value: "EOF",
}

const (
	LBRACE = "{"
	RBRACE = "}"

	LPAREN = "("
	RPAREN = ")"

	LBRACKET = "["
	RBRACKET = "]"

	QUOTE        = `"`
	SINGLEQUOTE  = "'"
	COLON        = ":"
	SEMICOLON    = ";"
	QUESTIONMARK = "?"
	PERIOD       = "."
	COMMA        = ","
	EQUALS       = "="
)

type Token struct {
	Type  TokenType
	Value string
}

var booleans = map[string]bool{
	"false": true,
	"true":  true,
}

var delimiters = map[byte]Token{
	'{': Delimiter(LBRACE),
	'}': Delimiter(RBRACE),
	'[': Delimiter(LBRACKET),
	']': Delimiter(RBRACKET),
	'(': Delimiter(LPAREN),
	')': Delimiter(RPAREN),
	':': Delimiter(COLON),
	';': Delimiter(SEMICOLON),
	'?': Delimiter(QUESTIONMARK),
	',': Delimiter(COMMA),
	'.': Delimiter(PERIOD),
	'=': Delimiter(EQUALS),
}

type tokenizer struct {
	s      string
	i      int
	tokens []Token
}

func parseLetters(t *tokenizer) {
	l := t.s[t.i]
	var sb strings.Builder

	for isLetter(l) || isNumeric(l) {
		sb.WriteByte(l)
		t.i++

		if t.i >= len(t.s) {
			break
		}
		l = t.s[t.i]
	}

	word := sb.String()
	if _, ok := keywords[word]; ok {
		t.tokens = append(t.tokens, Keyword(word))
	} else if _, ok := booleans[word]; ok {
		t.tokens = append(t.tokens, Token{
			Type:  BooleanValue,
			Value: word,
		})
	} else {
		t.tokens = append(t.tokens, Identifier(word))
	}
}

func parseNumber(t *tokenizer) {
	var sb strings.Builder
	l := t.s[t.i]

	for isNumeric(l) {
		sb.WriteByte(l)
		t.i++

		if t.i >= len(t.s) {
			break
		}
		l = t.s[t.i]
	}

	word := sb.String()
	t.tokens = append(t.tokens, Token{
		Type:  NumberValue,
		Value: word,
	})
}

func parseString(t *tokenizer) {
	c := t.s[t.i]

	t.i++
	l := t.s[t.i]

	var sb strings.Builder

	for t.i < len(t.s) {
		if l == c {
			t.tokens = append(t.tokens, Token{
				Type:  StringValue,
				Value: sb.String(),
			})
			break
		} else if t.i >= len(t.s) {
			t.tokens = append(t.tokens, EOFToken)
			break
		} else {
			sb.WriteByte(l)
		}

		t.i++
		l = t.s[t.i]
	}

	t.i++
}

func isString(c byte) bool {
	return c == '\'' || c == '"'
}

func isDelimiter(c byte) bool {
	_, ok := delimiters[c]
	return ok
}

func (t *tokenizer) tokenize() []Token {
	for t.i < len(t.s) {
		c := t.s[t.i]

		switch {
		case isLetter(c):
			parseLetters(t)
		case isNumeric(c):
			parseNumber(t)
		case isDelimiter(c):
			t.tokens = append(t.tokens, delimiters[c])
			t.i++
		case isString(c):
			parseString(t)
		default:
			t.i++
		}

	}

	t.tokens = append(t.tokens, EOFToken)

	return t.tokens
}

func tokenize(s string) []Token {
	t := tokenizer{s: s}
	return t.tokenize()
}

func isLetter(c byte) bool {
	for _, letter := range []byte("abcdefghijklmnoprqstuvwxyzABCDEFGHIJKLMNOPRQSTUVWXYZ") {
		if letter == c {
			return true
		}
	}

	return false
}

func isNumeric(c byte) bool {
	for _, i := range []byte("0123456789") {
		if i == c {
			return true
		}
	}

	return false
}

func parseData(tok Token) (map[string]interface{}, error) {
	d := map[string]interface{}{}
	err := json.Unmarshal([]byte(tok.Value), &d)
	return d, err
}

var keywords = map[string]bool{
	"SELECT":  true,
	"LAST":    true,
	"FIRST":   true,
	"SET":     true,
	"DELETE":  true,
	"UPDATE":  true,
	"CREATE":  true,
	"SCHEMA":  true,
	"WITH":    true,
	"IN":      true,
	"FROM":    true,
	"String":  true,
	"Number":  true,
	"Array":   true,
	"Object":  true,
	"Map":     true,
	"Boolean": true,
}

var commands = map[string]Command{
	"GET":    Get,
	"SET":    Set,
	"UPDATE": Update,
	"INFO":   Info,
	"CREATE": Create,
	"LAST":   Last,
	"FIRST":  First,
}
