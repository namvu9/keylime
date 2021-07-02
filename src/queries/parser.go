package queries

import (
	"encoding/json"
	"fmt"
	"strings"
)

type Command string

const (
	Set    Command = "Set"
	Get            = "Get"
	Update         = "Update"
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

type Mode func(*Parser)
type Parser struct {
	op     *Operation
	index  int
	mode   Mode
	modes  map[string]Mode
	Err    error
	input  string
	buffer strings.Builder
}

func parseCollection(p *Parser) {
	for !p.Done() {
		c := p.input[p.index]
		p.index++
		switch {
		case c == ' ':
			break
		default:
			p.buffer.WriteByte(c)
		}
	}

	if p.op.Collection != "" {
		p.Err = fmt.Errorf("syntax error: collection already set for this statement")
		return
	}
	s := p.buffer.String()
	p.op.Collection = s
	p.buffer = strings.Builder{}
}

func parseSetArguments(p *Parser) {
	for !p.Done() {
		c := p.input[p.index]
		p.index++

		if c == ' ' {
			s := p.buffer.String()
			p.op.Arguments["key"] = s
			return
		}

		p.buffer.WriteByte(c)
	}
}

func parsePayload(p *Parser) {
	var done bool

	leftBrackets := 0

	for !p.Done() {
		c := p.input[p.index]
		p.index++

		switch c {
		case '{':
			leftBrackets++
		case '}':
			leftBrackets--
			if leftBrackets == 0 {
				done = true
			}
		case ' ':
			if done {
				s := p.buffer.String()
				d := map[string]interface{}{}
				err := json.Unmarshal([]byte(s), &d)
				p.Err = err

				p.op.Payload.Data = d
				p.op.Payload.Format = "json"
				return
			}
		}

		p.buffer.WriteByte(c)

	}

}

func NewParser(input string) *Parser {
	p := &Parser{input: input, op: &Operation{
		Arguments: make(map[string]string),
	}}

	return p
}

func (p *Parser) Parse() (Operation, error) {
	for !p.Done() {
		c := p.input[p.index]
		p.index++

		if c == ' ' {
			s := p.buffer.String()
			p.buffer = strings.Builder{}

			switch s {
			case "IN":
				parseCollection(p)
				p.buffer = strings.Builder{}
			case "SET":
				p.op.Command = Set
				parseSetArguments(p)
				p.buffer = strings.Builder{}
			case "WITH":
				parsePayload(p)
				fmt.Println(p.op.Payload)
				p.buffer = strings.Builder{}
			}
		} else {
			p.buffer.WriteByte(c)
		}

	}
	return *p.op, p.Err
}

func (p *Parser) Done() bool {
	return p.index >= len(p.input) || p.Err != nil
}

func (p *Parser) setMode(m string) {
	p.mode = p.modes[m]
}
