package queries

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/namvu9/keylime/src/types"
)

func parseFieldName(p *Parser, sb *types.SchemaBuilder) (string, error) {
	if p.CurrentToken().Type != IdentifierToken {
		return "", fmt.Errorf("Schema syntax error: %v Expected Identifier got %v", p.CurrentToken(), p.Peek())
	}
	nameToken := p.CurrentToken()

	return nameToken.Value, nil
}

func parseFieldType(p *Parser, sb *types.SchemaBuilder) (types.Type, error) {
	tok := p.CurrentToken()
	if tok.Type != KeywordToken && tok.Value != LBRACKET && tok.Value != LBRACE {
		return "", fmt.Errorf("Expected Keyword or LBRACKET, got %v", p.CurrentToken())
	}
	switch p.CurrentToken().Value {
	case "Number":
		return types.Number, nil
	case "Map":
		return types.Map, nil
	case "Boolean":
		return types.Boolean, nil
	case "String":
		return types.String, nil
	case LBRACKET:
		if p.Peek().Value != RBRACKET {
			return types.Unknown, fmt.Errorf("Expected RBRACKET after LBRACKET got %s", p.Peek())
		}

		p.Next()
		return types.Array, nil
	case LBRACE:
		p.Prev()
		return types.Object, nil
	}

	return types.Unknown, fmt.Errorf("Unknown data type %s", p.CurrentToken().Value)
}

func parseDefaultValue(p *Parser) (interface{}, error) {
	tok := p.CurrentToken()
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
			return val, nil
		case BooleanValue:
			val, err := strconv.ParseBool(tok.Value)
			if err != nil {
				return nil, err
			}
			return val, nil
		case ArrayValue:
			var v []interface{}
			err := json.Unmarshal([]byte(tok.Value), &v)
			if err != nil {
				return nil, err
			}

			return v, nil
		case ObjectValue, MapValue:
			var v map[string]interface{}
			err := json.Unmarshal([]byte(tok.Value), &v)
			if err != nil {
				return nil, err
			}
			return v, nil
		}
	}

	return tok.Value, nil
}

func parseRange(p *Parser) (*types.SchemaFieldOption, error) {
	min := -1
	max := -1

	p.Next()

	if p.CurrentToken().Type == NumberValue {
		n, err := strconv.ParseInt(p.CurrentToken().Value, 0, 0)
		if err != nil {
			return nil, err
		}
		min = int(n)
		p.Next()
	}
	if p.CurrentToken().Value == COMMA {
		p.Next()
		if p.CurrentToken().Type == NumberValue {
			n, err := strconv.ParseInt(p.CurrentToken().Value, 0, 0)
			if err != nil {
				return nil, err
			}
			max = int(n)
			p.Next()
		}
	}

	if max <= min && max > -1 {
		return nil, fmt.Errorf("Range error: Max value must be greater than min value")
	}

	if min > -1 && max > -1 {
		opt := types.WithRange(min, max)
		return &opt, nil
	}

	if min > -1 {
		opt := types.WithMin(min)
		return &opt, nil
	}

	if max > -1 {
		opt := types.WithMax(max)
		return &opt, nil
	}

	return nil, nil
}

func parseSchema(p *Parser) (*types.Schema, error) {
	sb := types.NewSchemaBuilder()

	if p.CurrentToken().Value != LBRACE {
		return nil, fmt.Errorf("Schema syntax error: Could not find starting LBRACE (Got %v)", p.CurrentToken())
	}

	p.Next()

	for p.CurrentToken().Value != RBRACE {

		var schemaOptions []types.SchemaFieldOption

		name, err := parseFieldName(p, sb)
		if err != nil {
			return nil, err
		}

		p.Next()
		if p.CurrentToken().Value != COLON {
			return nil, fmt.Errorf("Expected COLON got %v", p.CurrentToken())
		}

		p.Next()

		kind, err := parseFieldType(p, sb)
		if err != nil {
			return nil, err
		}

		p.Next()

		if kind.Is(types.Object) {
			s, err := parseSchema(p)
			if err != nil {
				return nil, fmt.Errorf("%v", err)
			}
			schemaOptions = append(schemaOptions, types.WithSchema(s))
			p.Next()
		}

		if kind.Is(types.Array) {
			if p.CurrentToken().Value == LBRACE {
				s, err := parseSchema(p)
				if err != nil {
					return nil, err
				}

				schemaOptions = append(schemaOptions, types.WithElementType(types.Object))
				schemaOptions = append(schemaOptions, types.WithSchema(s))
			}
			if p.CurrentToken().IsDataType() {
				schemaOptions = append(schemaOptions, types.WithElementType(types.Type(p.CurrentToken().Value)))
				p.Next()
			}

			if p.CurrentToken().Value == LPAREN {
				rangeOpt, err := parseRange(p)
				if err != nil {
					return nil, err
				}

				schemaOptions = append(schemaOptions, *rangeOpt)
				p.Next()
			}
		}

		if kind == StringValue {
			if p.CurrentToken().Value == LPAREN {
				rangeOpt, err := parseRange(p)
				if err != nil {
					return nil, err
				}

				schemaOptions = append(schemaOptions, *rangeOpt)
				p.Next()
			}
		}

		if p.CurrentToken().Value == QUESTIONMARK {
			schemaOptions = append(schemaOptions, types.Optional)
			p.Next()
		}

		if p.CurrentToken().Value == EQUALS {
			p.Next()
			defaultValue, err := parseDefaultValue(p)
			if err != nil {
				return nil, err
			}

			schemaOptions = append(schemaOptions, types.WithDefault(defaultValue))
		}

		sb.AddField(name, kind, schemaOptions...)

		if p.CurrentToken().Value == COMMA {
			p.Next()
		}
	}

	schema, err := sb.Build()
	if err != nil {
		return nil, fmt.Errorf("%v", err)
	}

	return schema, nil
}
