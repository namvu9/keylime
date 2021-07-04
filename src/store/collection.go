package store

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"sync"

	"github.com/namvu9/keylime/src/errors"
	"github.com/namvu9/keylime/src/types"
)

//A collection is a named container for a group of records
type collection struct {
	Name      string
	HasSchema bool
	Schema    *types.Schema

	primaryIndex *KeyIndex
	orderIndex   *OrderIndex
	storage      ReadWriterTo
	loaded       bool
}

// Get the value associated with the key `k`, if a record
// with that key exists. Otherwise, nil is returned
func (c *collection) Get(ctx context.Context, k string) (types.Document, error) {
	err := c.load()
	if err != nil {
		return types.NewDoc(k), err
	}

	r, err := c.primaryIndex.get(ctx, k)
	if err != nil {
		return *r, err
	}

	if c.Schema != nil {
		doc := c.Schema.WithDefaults(*r)
		return doc, nil
	}

	return *r, nil
}

type Fields = map[string]interface{}

// Set the value associated with key `k` in collection `c`.
// If a record with that key already exists in the
// collection, an error is returned.
func (c *collection) Set(ctx context.Context, k string, fields Fields) error {
	err := c.load()
	if err != nil {
		return err
	}

	err = c.set(ctx, k, fields)
	if err != nil {
		return err
	}

	return c.commit()
}

// TODO: Test that Insertion of the same doc is idempotent
// with respect to the key
func (c *collection) set(ctx context.Context, k string, fields Fields) error {
	wrapError := errors.WrapWith("(*Collection).Set", errors.EInternal)
	doc := types.NewDoc(k).Set(fields)

	if c.Schema != nil {
		err := c.Schema.Validate(doc)
		if err != nil {
			return wrapError(err)
		}
	}

	if old, err := c.primaryIndex.insert(ctx, doc); err != nil {
		return err
	} else if old != nil {
		return c.orderIndex.update(ctx, doc)
	}
	fmt.Println("HAHA")

	return c.orderIndex.insert(ctx, doc)
}

func (c *collection) commit() error {
	errChan := make(chan error, 2)
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := c.primaryIndex.save(); err != nil {
			errChan <- err
			return
		}

		if err := c.primaryIndex.bufWriter.flush(); err != nil {
			errChan <- err
		}
	}()

	go func() {
		defer wg.Done()
		if err := c.orderIndex.save(); err != nil {
			errChan <- err
			return
		}

		if err := c.orderIndex.writer.flush(); err != nil {
			errChan <- err
		}
	}()

	wg.Wait()

	if len(errChan) != 0 {
		return <-errChan
	}

	return nil
}

func (c *collection) GetLast(ctx context.Context, n int) ([]types.Document, error) {
	err := c.load()
	if err != nil {
		return []types.Document{}, err
	}

	// TODO: return error from oi
	return c.orderIndex.Get(n, false), nil
}

func (c *collection) GetFirst(ctx context.Context, n int) ([]types.Document, error) {
	err := c.load()
	if err != nil {
		return []types.Document{}, err
	}

	// TODO: return error from oi
	return c.orderIndex.Get(n, true), nil
}

func (c *collection) Update(ctx context.Context, k string, fields map[string]interface{}) error {
	err := c.load()
	if err != nil {
		return err
	}

	err = c.update(ctx, k, fields)
	if err != nil {
		return err
	}

	return c.commit()
}

func (c *collection) update(ctx context.Context, k string, fields map[string]interface{}) error {
	// Retrieve record
	wrapError := errors.WrapWith("(*Collection).Update", errors.EInternal)
	r, err := c.primaryIndex.get(ctx, k)
	if err != nil {
		return wrapError(err)
	}

	clone := r.Update(fields)
	if c.Schema != nil {
		err := c.Schema.Validate(clone)
		if err != nil {
			return wrapError(err)
		}
	}

	_, err = c.primaryIndex.insert(ctx, clone)
	if err != nil {
		wrapError(err)
	}

	err = c.orderIndex.update(ctx, clone)
	if err != nil {
		wrapError(err)
	}

	return nil
}

// TODO: If this fails, clean up
func (c *collection) Create(ctx context.Context, s *types.Schema) error {
	var op errors.Op = "(*Collection).Create"

	_, err := c.storage.Write(nil)
	if err != nil {
		return errors.Wrap(op, errors.EIO, err)
	}

	c.Schema = s
	if s != nil {
		c.HasSchema = true
	}

	err = c.primaryIndex.create()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	err = c.orderIndex.create()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	err = c.save()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	fmt.Println("CREATED collection", c.orderIndex.Head)

	return nil
}

func (c *collection) load() error {
	if c.loaded {
		return nil
	}

	if !c.exists() {
		return fmt.Errorf("Collection %s does not exist", c.Name)
	}

	var op errors.Op = "(*Collection).Load"
	err := c.primaryIndex.Load()

	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	err = c.orderIndex.load()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	schemaReader := c.storage.WithSegment("schema")
	ok, err := schemaReader.Exists()
	if err != nil {
		return errors.Wrap(op, errors.EIO, err)
	}

	if ok {
		data, err := io.ReadAll(schemaReader)
		if err != nil {
			return errors.Wrap(op, errors.EIO, err)
		}

		s := types.NewSchema()

		buf := bytes.NewBuffer(data)
		dec := gob.NewDecoder(buf)
		err = dec.Decode(&s)
		if err != nil {
			return errors.Wrap(op, errors.EIO, err)
		}

		c.Schema = s
	}

	c.loaded = true

	return nil
}

// Delete record with key `k`. An error is returned of no
// such record exists
func (c *collection) Delete(ctx context.Context, k string) error {
	err := c.load()
	if err != nil {
		return err
	}

	err = c.remove(ctx, k)
	if err != nil {
		return err
	}

	return c.commit()
}

func (c *collection) remove(ctx context.Context, k string) error {
	var op errors.Op = "(*Collection).Delete"

	err := c.primaryIndex.remove(ctx, k)
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	err = c.primaryIndex.save()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	err = c.orderIndex.remove(ctx, k)
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	err = c.orderIndex.save()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	return err
}

func (c *collection) save() error {
	wrapError := errors.WrapWith("(*Collection).Save", errors.EIO)

	if c.Schema != nil {
		w := c.storage.WithSegment("schema")
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err := enc.Encode(c.Schema)
		if err != nil {
			return wrapError(err)
		}

		_, err = w.Write(buf.Bytes())
		if err != nil {
			return wrapError(err)
		}
	}

	return nil
}

func (c *collection) Info(ctx context.Context) {
	fmt.Println()
	fmt.Println("---------------")
	fmt.Println("Collection:", c.Name)
	fmt.Println("---------------")

	if c.Schema != nil {
		fmt.Println(c.Schema)
	}
	c.primaryIndex.Info()
	fmt.Println()
	c.orderIndex.Info()
	fmt.Println()
}

func (c *collection) exists() bool {
	if ok, err := c.storage.Exists(); !ok || err != nil {
		return false
	}

	return true

}

func newCollection(name string, s ReadWriterTo) *collection {
	t := 50
	c := &collection{
		Name:    name,
		storage: newIOReporter(),
	}

	if s != nil {
		c.storage = s.WithSegment(name)
		c.primaryIndex =
			newKeyIndex(t, s.WithSegment(name))
		c.orderIndex = newOrderIndex(t*2, s.WithSegment(name))
	} else {
		c.primaryIndex = newKeyIndex(t, c.storage)
		c.orderIndex = newOrderIndex(t*2, c.storage)
	}

	return c
}
