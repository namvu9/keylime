package queries

import (
	"encoding/json"
	"strings"
)

type TokenType string

const (
	Delimiter    TokenType = "Delimiter"
	Keyword                = "Keyword"
	Identifier             = "Identifier"
	StringValue            = "String"
	BooleanValue           = "Boolean"
	NumberValue            = "Number"
	ArrayValue             = "Array"
	ObjectValue            = "Object"
	MapValue               = "Map"
	EOF                    = "EOF"
)

func (t Token) IsValueType() bool {
	switch t.Type {
	case StringValue, BooleanValue, NumberValue, ArrayValue, ObjectValue, MapValue:
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

func tokenize(s string) []Token {
	newTokens := []Token{}

	i := 0
	for i < len(s) {
		c := s[i]

		if isLetter(c) {
			var sb strings.Builder
			l := c

			for isLetter(l) || isNumeric(l) {
				sb.WriteByte(l)
				i++

				if i >= len(s) {
					break
				}
				l = s[i]
			}

			word := sb.String()
			if _, ok := keywords[word]; ok {
				newTokens = append(newTokens, Token{
					Type:  Keyword,
					Value: word,
				})
			} else if _, ok := booleans[word]; ok {
				newTokens = append(newTokens, Token{
					Type:  BooleanValue,
					Value: word,
				})
			} else {
				newTokens = append(newTokens, Token{
					Type:  Identifier,
					Value: word,
				})
			}
		} else if isNumeric(c) {
			var sb strings.Builder
			l := c

			for isNumeric(l) {
				sb.WriteByte(l)
				i++

				if i >= len(s) {
					break
				}
				l = s[i]
			}

			word := sb.String()
			newTokens = append(newTokens, Token{
				Type:  NumberValue,
				Value: word,
			})

		} else {
			switch c {
			case '{':
				newTokens = append(newTokens, Token{
					Type:  Delimiter,
					Value: LBRACE,
				})
			case '}':
				newTokens = append(newTokens, Token{
					Type:  Delimiter,
					Value: RBRACE,
				})
			case '[':
				newTokens = append(newTokens, Token{
					Type:  Delimiter,
					Value: LBRACKET,
				})
			case ']':
				newTokens = append(newTokens, Token{
					Type:  Delimiter,
					Value: RBRACKET,
				})
			case '(':
				newTokens = append(newTokens, Token{
					Type:  Delimiter,
					Value: LPAREN,
				})
			case ')':
				newTokens = append(newTokens, Token{
					Type:  Delimiter,
					Value: RPAREN,
				})
			case ':':
				newTokens = append(newTokens, Token{
					Type:  Delimiter,
					Value: COLON,
				})
			case ';':
				newTokens = append(newTokens, Token{
					Type:  Delimiter,
					Value: SEMICOLON,
				})
			case '?':
				newTokens = append(newTokens, Token{
					Type:  Delimiter,
					Value: QUESTIONMARK,
				})
			case ',':
				newTokens = append(newTokens, Token{
					Type:  Delimiter,
					Value: COMMA,
				})
			case '.':
				newTokens = append(newTokens, Token{
					Type:  Delimiter,
					Value: PERIOD,
				})
			case '=':
				newTokens = append(newTokens, Token{
					Type:  Delimiter,
					Value: EQUALS,
				})
			case '\'', '"':
				i++
				l := s[i]

				var sb strings.Builder

				for i < len(s) {
					if l == c {
						newTokens = append(newTokens, Token{
							Type:  StringValue,
							Value: sb.String(),
						})
						break
					} else if i >= len(s) {
						newTokens = append(newTokens, EOFToken)
						break
					} else {
						sb.WriteByte(l)
					}

					i++
					l = s[i]
				}

			}

			i++
		}

	}

	newTokens = append(newTokens, EOFToken)

	return newTokens
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
