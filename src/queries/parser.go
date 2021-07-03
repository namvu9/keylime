package queries

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/namvu9/keylime/src/types"
)

type Command string

const (
	Set    Command = "Set"
	Get            = "Get"
	Update         = "Update"
	Info           = "Info"
	Create         = "Create"
)

// Operation is an intermediate representation of the
// KeyLime query "language" and is interpretable by the
// KeyLime store
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
	Err    error
	input  string
	buffer strings.Builder

	tokens []Token
}

func parseSchema(p *Parser) (*types.Schema, error) {
	sb := types.NewSchemaBuilder()

	if p.CurrentToken().Value != LBRACE {
		return nil, fmt.Errorf("Schema syntax error: Could not find starting LBRACE (Got %v)", p.CurrentToken())
	}

	p.Next()
	for p.CurrentToken().Value != RBRACE {
		if p.CurrentToken().Value == COMMA {
			p.Next()
		}

		if p.CurrentToken().Type != Identifier {
			return nil, fmt.Errorf("Schema syntax error: %v Expected Identifier got %v", p.CurrentToken(), p.Peek())
		}
		nameToken := p.CurrentToken()

		var fieldType types.Type
		var schemaOptions []types.SchemaFieldOption


		tok := p.Next()
		if tok.Value == QUESTIONMARK {
			schemaOptions = append(schemaOptions, types.Optional)
			tok = p.Next()
		}

		if tok.Value != COLON {
			return nil, fmt.Errorf("Schema syntax error: Expected Colon token got %s", tok.Value)
		}

		tok = p.Next()
		if tok.Value == LBRACE {
			s, err := parseSchema(p)
			if err != nil {
				return nil, fmt.Errorf("%v", err)
			}
			schemaOptions = append(schemaOptions, types.WithSchema(s))
			fieldType = types.Object
			// ...
		} else if tok.Value == LBRACKET {
			fieldType = types.Array

		} else if tok.Type != Keyword {
			return nil, fmt.Errorf("Schem syntax error: Expected Keyword token got %s", tok.Type)
		} else {
			switch tok.Value {
			case "Number":
				fieldType = types.Number
			case "Map":
				fieldType = types.Map
			case "Boolean":
				fieldType = types.Boolean
			case "String":
				fieldType = types.String
			}
		}

		tok = p.Next()
		if tok.Value == EQUALS {
			tok = p.Next()
			if !tok.IsValueType() {
				return nil, fmt.Errorf("Schema syntax error: Expected value type, got %s", tok.Type)
			}
			p.Next()

			if tok.Type != StringValue {
				switch tok.Type {
				case NumberValue:
					val, err := strconv.ParseFloat(tok.Value, 64)
					if err != nil {
						return nil, err
					}
					schemaOptions = append(schemaOptions, types.WithDefault(val))
				case BooleanValue:
					val, err := strconv.ParseBool(tok.Value)
					if err != nil {
						return nil, err
					}
					schemaOptions = append(schemaOptions, types.WithDefault(val))
				case ArrayValue:
					var v []interface{}
					err := json.Unmarshal([]byte(tok.Value), &v)
					if err != nil {
						return nil, err
					}

					schemaOptions = append(schemaOptions, types.WithDefault(v))
				case ObjectValue, MapValue:
					var v map[string]interface{}
					err := json.Unmarshal([]byte(tok.Value), &v)
					if err != nil {
						return nil, err
					}

					schemaOptions = append(schemaOptions, types.WithDefault(v))

				}
			} else {
				schemaOptions = append(schemaOptions, types.WithDefault(tok.Value))
			}
		}

		sb.AddField(nameToken.Value, fieldType, schemaOptions...)
	}

	schema, err := sb.Build()
	if err != nil {
		return nil, fmt.Errorf("%v", err)
	}
	fmt.Println("AFTER PARSING SCHEMA", p.CurrentToken())

	return schema, nil
}

func (p *Parser) Parse() (Operation, error) {
	for token := p.tokens[p.index]; token.Type != "EOF"; token = p.Next() {
		switch token.Value {
		case "SEMICOLON":
			break
		case "DELETE":
			if p.Peek().Type != Identifier {
				return *p.op, fmt.Errorf("Parsing error: Expected Argument token after DELETE, but got =%v", p.Peek())
			}

			p.op.Command = commands[token.Value]
			next := p.Next()

			p.op.Arguments["key"] = next.Value

		case "WITH":
			if p.Peek().Type != StringValue && p.Peek().Value != "SCHEMA" {
				return *p.op, fmt.Errorf("Parsing error: Expected StringValue token after WITH, but got %s", p.Peek().Type)
			}

			if p.Peek().Type == Keyword && p.Peek().Value == "SCHEMA" {
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
			if p.Peek().Type != Identifier {
				return *p.op, fmt.Errorf("Parsing error: Expected Identifier token after CREATE, but got =%v", p.Peek())
			}

			p.op.Command = Create
			next := p.Next()
			p.op.Collection = next.Value
			fmt.Println("CREATING", next)

		case "INFO":
			if p.Peek().Type != Identifier {
				return *p.op, fmt.Errorf("Parsing error: Expected Identifier token after INFO, but got =%v", p.Peek())
			}

			p.op.Command = commands[token.Value]
			next := p.Next()
			p.op.Collection = next.Value

		case "SET", "UPDATE":
			if p.Peek().Type != Identifier {
				return *p.op, fmt.Errorf("Parsing error: Expected Identifier token after SET, but got =%v", p.Peek())
			}

			if len(p.op.Payload.Data) == 0 {
				return *p.op, fmt.Errorf("Parsing error: The %s command requires a payload", token.Value)
			}

			p.op.Command = commands[token.Value]
			next := p.Next()

			p.op.Arguments["key"] = next.Value

		case "GET":
			if p.Peek().Type != Identifier {
				return *p.op, fmt.Errorf("Parsing error: Expected Identifier token after GET, but got =%v", p.Peek().Type)
			}

			p.op.Command = commands[token.Value]
			next := p.Next()

			argString := next.Value
			if p.Peek().Value == "FROM" {
				p.op.Arguments["selectors"] = argString
				p.Next()
				if p.Peek().Type != Identifier {
					return *p.op, fmt.Errorf("Parsing error: Expected Argument token after SET, but got =%v", p.Peek())
				}
				next := p.Next()
				p.op.Arguments["key"] = next.Value
			} else {
				p.op.Arguments["key"] = argString
			}

		case "IN":
			if p.Peek().Type != Identifier {
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

func (p *Parser) Peek() Token {
	if p.index+1 >= len(p.tokens) {
		return EOFToken
	}
	t := p.tokens[p.index+1]

	return t
}

func NewParser(input string) *Parser {
	tokens := tokenize(input)
	p := &Parser{
		input:  input,
		tokens: tokens,
		op: &Operation{
			Arguments: make(map[string]string),
		}}

	return p
}
