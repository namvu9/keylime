// TODO: Rename to queries
package queries

import (
	"context"
	"fmt"
	"strconv"

	"github.com/namvu9/keylime/src/errors"
	"github.com/namvu9/keylime/src/types"
)

func Interpret(ctx context.Context, s types.Store, input string) (interface{}, error) {
	op, err := Parse(input)
	if err != nil {
		return nil, err
	}

	handler, ok := handlers[op.Command]
	if !ok {
		return nil, fmt.Errorf("Unknown command: %s", op.Command)
	}

	return handler(ctx, s, *op)
}

type cmdHandler func(context.Context, types.Store, Operation) (interface{}, error)

var handlers = map[Command]cmdHandler{
	Get:    handleGet,
	Set:    handleSet,
	Update: handleUpdate,
	Create: handleCreate,
	Delete: handleDelete,
	First:  handleFirst,
	Last:   handleLast,
	Info:   handleInfo,
}

func handleGet(ctx context.Context, s types.Store, op Operation) (interface{}, error) {
	c := s.Collection(op.Collection)

	key := op.Arguments["key"]
	rec, err := c.Get(ctx, key)
	if err != nil {
		werr := errors.Wrap("(*types.Store).Run", errors.ENotFound, fmt.Errorf("%w in %s", err, op.Collection))
		werr.Collection = op.Collection

		return nil, werr
	}

	if selectors, ok := op.Arguments["selectors"]; ok {
		res := rec.Select(types.MakeFieldSelectors(selectors)...)
		return res, nil
	}

	return rec, err
}

func handleSet(ctx context.Context, s types.Store, op Operation) (interface{}, error) {
	c := s.Collection(op.Collection)
	key := op.Arguments["key"]
	fields := op.Payload.Data

	err := c.Set(ctx, key, fields)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func handleUpdate(ctx context.Context, s types.Store, op Operation) (interface{}, error) {
	var (
		c      = s.Collection(op.Collection)
		key    = op.Arguments["key"]
		fields = op.Payload.Data
	)

	err := c.Update(ctx, key, fields)

	return nil, err
}

func handleCreate(ctx context.Context, s types.Store, op Operation) (interface{}, error) {
	c := s.Collection(op.Collection)

	if schema, ok := op.Payload.Data["schema"]; ok {
		err := c.Create(ctx, schema.(*types.Schema))
		if err != nil {
			return nil, err
		}
	} else {
		err := c.Create(ctx, nil)
		if err != nil {
			return nil, err
		}
	}

	return nil, nil
}

func handleDelete(ctx context.Context, s types.Store, op Operation) (interface{}, error) {
	var (
		c   = s.Collection(op.Collection)
		key = op.Arguments["key"]
	)

	err := c.Delete(ctx, key)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func handleFirst(ctx context.Context, s types.Store, op Operation) (interface{}, error) {
	c := s.Collection(op.Collection)
	n, err := strconv.ParseInt(op.Arguments["n"], 0, 0)
	if err != nil {
		return nil, err
	}

	return c.GetFirst(ctx, int(n))
}

func handleLast(ctx context.Context, s types.Store, op Operation) (interface{}, error) {
	c := s.Collection(op.Collection)
	n, err := strconv.ParseInt(op.Arguments["n"], 0, 0)
	if err != nil {
		return nil, err
	}

	return c.GetLast(ctx, int(n))
}

func handleInfo(ctx context.Context, s types.Store, op Operation) (interface{}, error) {
	c := s.Collection(op.Collection)
	c.Info(ctx)
	return nil, nil
}
