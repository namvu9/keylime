package queries

import (
	"fmt"
	"strings"
)

type Command string

const (
	Set    Command = "Set"
	Get            = "Get"
	Update         = "Update"
	Info           = "Info"
	Create         = "Create"
	Last           = "Last"
	First          = "First"
	Delete         = "Delete"
)

type Operation struct {
	// The target collection of the operation
	Collection string

	// The action to perform
	Command Command

	Arguments map[string]string

	// Data
	Payload struct {
		Data   map[string]interface{}
		Format string
	}
}

type Parser struct {
	op     *Operation
	index  int
	buffer strings.Builder

	tokens []Token
}

// TODO: Test
// TODO: TEST
func (p *Parser) Parse() (Operation, error) {
	for token := p.tokens[p.index]; token.Type != "EOF"; token = p.Next() {
		switch token.Value {
		case "SEMICOLON":
			break
		case "FIRST":
			p.op.Command = First

			if p.Peek().Type != NumberValue {
				return *p.op, fmt.Errorf("Parsing error: Expected Number token after FIRST but got %v", p.Peek())
			}

			n := p.Next()
			p.op.Arguments["n"] = n.Value
		case "LAST":
			p.op.Command = Last

			if p.Peek().Type != NumberValue {
				return *p.op, fmt.Errorf("Parsing error: Expected Number token after LAST but got %v", p.Peek())
			}
			n := p.Next()
			p.op.Arguments["n"] = n.Value

		case "DELETE":
			p.op.Command = Delete

			if p.Peek().Type != IdentifierToken {
				return *p.op, fmt.Errorf("Parsing error: Expected Argument token after DELETE, but got =%v", p.Peek())
			}

			next := p.Next()

			p.op.Arguments["key"] = next.Value

		case "WITH":
			if p.Peek().Type != StringValue && p.Peek().Value != "SCHEMA" {
				return *p.op, fmt.Errorf("Parsing error: Expected StringValue token after WITH, but got %s", p.Peek().Type)
			}

			if p.Peek().Type == KeywordToken && p.Peek().Value == "SCHEMA" {
				p.Next()
				p.Next()
				schema, err := parseSchema(p)
				if err != nil {
					return *p.op, err
				}

				p.op.Payload.Data = make(map[string]interface{})
				p.op.Payload.Data["schema"] = schema

			} else {
				next := p.Next()
				data, err := parseData(next)
				if err != nil {
					return *p.op, err
				}

				p.op.Payload.Data = data
			}
		case "CREATE":
			p.op.Command = Create

			if p.Peek().Type != IdentifierToken {
				return *p.op, fmt.Errorf("Parsing error: Expected Identifier token after CREATE, but got =%v", p.Peek())
			}

			next := p.Next()
			p.op.Collection = next.Value

		case "INFO":
			p.op.Command = Info

			if p.Peek().Type != IdentifierToken {
				return *p.op, fmt.Errorf("Parsing error: Expected Identifier token after INFO, but got =%v", p.Peek())
			}

			next := p.Next()
			p.op.Collection = next.Value

		case "SET", "UPDATE":
			p.op.Command = commands[token.Value]

			if p.Peek().Type != IdentifierToken {
				return *p.op, fmt.Errorf("Parsing error: Expected Identifier token after SET, but got =%v", p.Peek())
			}

			if len(p.op.Payload.Data) == 0 {
				return *p.op, fmt.Errorf("Parsing error: The %s command requires a payload", token.Value)
			}

			next := p.Next()

			p.op.Arguments["key"] = next.Value

		case "FROM":
			if p.Peek().Type != IdentifierToken {
				return *p.op, fmt.Errorf("Parsing error: Expected Identifier token after FROM, but got =%v", p.Peek().Type)
			}

			next := p.Next()
			p.op.Arguments["key"] = next.Value

		case "GET":
			p.op.Command = Get

			if p.Peek().Type != IdentifierToken {
				return *p.op, fmt.Errorf("Parsing error: Expected Identifier token after GET, but got =%v", p.Peek().Type)
			}

			next := p.Next()

			if _, ok := p.op.Arguments["key"]; !ok {
				p.op.Arguments["key"] = next.Value
			} else {
				selectors := []string{p.CurrentToken().Value}

				for p.Peek().Type == IdentifierToken {
					next := p.Next()
					selectors = append(selectors, next.Value)
				}

				if p.Peek().Value != "IN" {
					return *p.op, fmt.Errorf("Parsing error: Expected Keyword IN, but got =%v", p.CurrentToken())
				}

				p.op.Arguments["selectors"] = strings.Join(selectors, " ")
			}

		case "IN":
			if p.Peek().Type != IdentifierToken {
				return *p.op, fmt.Errorf("Parsing error: Expected Identifier token after IN, but got =%v", p.Peek())
			}

			next := p.Next()
			p.op.Collection = next.Value

		}

	}

	return *p.op, nil
}

func (p *Parser) CurrentToken() Token {
	if p.index >= len(p.tokens) {
		return EOFToken
	}
	return p.tokens[p.index]
}

func (p *Parser) Next() Token {
	p.index++
	if p.index >= len(p.tokens) {
		return EOFToken
	}
	token := p.tokens[p.index]
	return token
}

func (p *Parser) Prev() Token {
	p.index--
	token := p.tokens[p.index]
	return token
}

func (p *Parser) Peek() Token {
	if p.index+1 >= len(p.tokens) {
		return EOFToken
	}
	t := p.tokens[p.index+1]

	return t
}

func parseTokens(tokens []Token) (*Operation, error) {
	p := &Parser{
		tokens: tokens,
		op: &Operation{
			Arguments: make(map[string]string),
		}}
	op, err := p.Parse()

	if err != nil {
		return nil, err
	}

	return &op, err
}

func Parse(input string) (*Operation, error) {
	tokens := tokenize(input)
	return parseTokens(tokens)
}
