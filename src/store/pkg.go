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

func (s *Store) Run(ctx context.Context, op queries.Operation) (*types.Record, error) {
	c, err := s.Collection(op.Collection)
	if err != nil {
		return nil, err
	}

	if op.Command == queries.Create {
		// has schema?
		if op.Payload.Data != nil {
			fmt.Println(op.Payload.Data["schema"])
			return nil, nil
		}

		err := c.Create(nil)
		if err != nil {
			return nil, err
		}

		fmt.Printf("Successfully created collection %s\n", op.Collection)
		return nil, nil

	} else if !c.Exists() {
		return nil, fmt.Errorf("Collection %s does not exist", op.Collection)
	} else {
		err := c.Load()
		if err != nil {
			return nil, err
		}
	}

	switch op.Command {
	case queries.Info:
		c.Info()
		return nil, err
	case queries.Set:
		key := op.Arguments["key"]
		fields := op.Payload.Data

		err = c.Set(ctx, key, fields)
		if err != nil {
			return nil, err
		}

		err = c.Commit()
		if err != nil {
			return nil, err
		}
		fmt.Println("Successfully saved record with key", key, "in collection", op.Collection)
		return nil, nil
	case queries.Update:
		key := op.Arguments["key"]
		fields := op.Payload.Data
		err = c.Update(ctx, key, fields)
		if err != nil {
			return nil, err
		}

		err = c.Commit()
		if err != nil {
			return nil, err
		}
		fmt.Println("Successfully saved record with key", key, "in collection", op.Collection)
		return nil, nil

	case queries.Get:
		key := op.Arguments["key"]
		rec, err := c.Get(ctx, key)
		if err != nil {
			werr := errors.Wrap("(*Store).Run", errors.ENotFound, fmt.Errorf("%w in %s", err, op.Collection))
			werr.Collection = op.Collection

			return nil, werr
		}

		if selectors, ok := op.Arguments["selectors"]; ok {
			fmt.Println("TODO", selectors)
			//selectors := types.MakeFieldSelectors(args[1:]...)
			//res := rec.Select(selectors...)
			//s, _ := types.Prettify(res)
			//fmt.Printf("%s=%s\n", key, s)
		} else {
			return rec, err
		}
	}

	return nil, fmt.Errorf("Unknown command: %s", op.Command)
}

func (s Store) Collection(name string) (*Collection, error) {
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
			c, _ := s.Collection(f.Name())
			c.Info()
		}
	}
}
