/*
Package store implements a key-value store backed
by a B-tree.

*/
package store

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"

	"github.com/namvu9/keylime/src/errors"
	"github.com/namvu9/keylime/src/queries"
	"github.com/namvu9/keylime/src/types"
)

type ReadWriterTo interface {
	io.ReadWriter
	WithSegment(pathSegment string) ReadWriterTo
	Delete() error
	Exists() (bool, error)
}

type Store struct {
	initialized bool
	baseDir     string
	t           int
	collections map[string]*Collection

	storage ReadWriterTo
}

func (s *Store) Run(ctx context.Context, op queries.Operation) (interface{}, error) {
	c, err := s.collection(op.Collection)
	if err != nil {
		return nil, err
	}

	if op.Command == queries.Create {
		if schema := op.Payload.Data["schema"]; schema != nil {
			err := c.create(schema.(*types.Schema))
			if err != nil {
				return nil, err
			}
		} else {
			err := c.create(nil)
			if err != nil {
				return nil, err
			}
		}

		fmt.Printf("Successfully created collection %s\n", op.Collection)
		return nil, nil

	} else if !c.exists() {
		return nil, fmt.Errorf("Collection %s does not exist", op.Collection)
	} else {
		err := c.load()
		if err != nil {
			return nil, err
		}
	}

	switch op.Command {
	case queries.Delete:
		key := op.Arguments["key"]

		err = c.remove(ctx, key)
		if err != nil {
			return nil, err
		}

		err = c.commit()
		if err != nil {
			return nil, err
		}
		fmt.Println("Successfully deleted record with key", key, "in collection", op.Collection)
		return nil, nil

	case queries.First:
		n, err := strconv.ParseInt(op.Arguments["n"], 0, 0)
		if err != nil {
			return nil, err
		}

		records := c.getFirst(ctx, int(n))
		return records, nil
	case queries.Last:
		n, err := strconv.ParseInt(op.Arguments["n"], 0, 0)
		if err != nil {
			return nil, err
		}

		records := c.getLast(ctx, int(n))
		return records, nil
	case queries.Info:
		c.info()
		return nil, err
	case queries.Set:
		key := op.Arguments["key"]
		fields := op.Payload.Data

		err = c.set(ctx, key, fields)
		if err != nil {
			return nil, err
		}

		err = c.commit()
		if err != nil {
			return nil, err
		}
		fmt.Println("Successfully saved record with key", key, "in collection", op.Collection)
		return nil, nil
	case queries.Update:
		key := op.Arguments["key"]
		fields := op.Payload.Data
		err = c.update(ctx, key, fields)
		if err != nil {
			return nil, err
		}

		err = c.commit()
		if err != nil {
			return nil, err
		}
		fmt.Println("Successfully saved record with key", key, "in collection", op.Collection)
		return nil, nil

	case queries.Get:
		key := op.Arguments["key"]
		rec, err := c.get(ctx, key)
		if err != nil {
			werr := errors.Wrap("(*Store).Run", errors.ENotFound, fmt.Errorf("%w in %s", err, op.Collection))
			werr.Collection = op.Collection

			return nil, werr
		}

		if selectors, ok := op.Arguments["selectors"]; ok {
			selectors := types.MakeFieldSelectors(strings.Split(selectors, " ")...)
			res := rec.Select(selectors...)
			return res, nil
		} else {
			return rec, err
		}
	}

	return nil, fmt.Errorf("Unknown command: %s", op.Command)
}

func (s Store) collection(name string) (*Collection, error) {
	if name == "" {
		return nil, fmt.Errorf("collection names cannot be empty")
	}
	//var op errors.Op = "(Store).Collection"

	c, ok := s.collections[name]
	if !ok {
		c := newCollection(name, s.storage)
		s.collections[name] = c
		return c, nil
	}

	return c, nil
}

type Option func(*Store)

func WithStorage(rw ReadWriterTo) Option {
	return func(s *Store) {
		s.storage = rw
	}
}

func (s *Store) hasCollection(name string) bool {
	if ok, err := s.storage.WithSegment(name).Exists(); !ok || err != nil {
		return false
	}

	return true
}

func (s *Store) Info() {
	files, _ := ioutil.ReadDir(s.baseDir)
	for _, f := range files {
		if f.IsDir() {
			c, _ := s.collection(f.Name())
			c.info()
		}
	}
}
