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

// A Collection is a named container for a group of records
type Collection struct {
	Name   string
	Schema *types.Schema

	primaryIndex *KeyIndex
	orderIndex   *OrderIndex
	storage      ReadWriterTo
}

// Get the value associated with the key `k`, if a record
// with that key exists. Otherwise, nil is returned
func (c *Collection) Get(ctx context.Context, k string) (*types.Record, error) {
	r, err := c.primaryIndex.Get(ctx, k)
	if err != nil {
		return nil, err
	}

	if c.Schema != nil {
		return c.Schema.WithDefaults(r), nil
	}

	return r, nil
}

type Fields = map[string]interface{}

// Set the value associated with key `k` in collection `c`.
// If a record with that key already exists in the
// collection, an error is returned.
func (c *Collection) Set(ctx context.Context, k string, fields Fields) error {
	var wg sync.WaitGroup

	wrapError := errors.WrapWith("(*Collection).Set", errors.EInternal)
	r := types.NewRecord(k)
	r.SetFields(fields)

	if c.Schema != nil {
		err := c.Schema.Validate(r)
		if err != nil {
			return wrapError(err)
		}
	}

	wg.Add(2)
	ctx, cancelFn := context.WithCancel(ctx)
	errChan := make(chan error, 2)
	go func() {
		defer wg.Done()
		// TODO: use ctx cancelFunc
		if err := c.primaryIndex.Insert(ctx, *r); err != nil {
			errChan <- err
			cancelFn()
			return
		}
	}()

	go func() {
		defer wg.Done()

		if err := c.orderIndex.Insert(ctx, r); err != nil {
			errChan <- err
			return
		}

	}()

	wg.Wait()

	select {
	case e := <-errChan:
		return e
	default:
		return nil
	}
}

func (c *Collection) Commit() error {
	errChan := make(chan error, 2)
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := c.primaryIndex.Save(); err != nil {
			errChan <- err
			return
		}
		if err := c.primaryIndex.bufWriter.Flush(); err != nil {
			errChan <- err
			return
		}
	}()

	go func() {
		defer wg.Done()
		if err := c.orderIndex.Save(); err != nil {
			errChan <- err
			return
		}

		if err := c.orderIndex.writer.Flush(); err != nil {
			errChan <- err
			return
		}
	}()

	wg.Wait()

	return nil
}

func (c *Collection) GetLast(ctx context.Context, n int) []*types.Record {
	return c.orderIndex.Get(n, false)
}

func (c *Collection) GetFirst(ctx context.Context, n int) []*types.Record {
	return c.orderIndex.Get(n, true)
}

func (c *Collection) Update(ctx context.Context, k string, fields map[string]interface{}) error {
	// Retrieve record
	wrapError := errors.WrapWith("(*Collection).Update", errors.EInternal)
	r, err := c.primaryIndex.Get(ctx, k)
	if err != nil {
		return wrapError(err)
	}

	clone := r.UpdateFields(fields)
	if c.Schema != nil {
		err := c.Schema.Validate(clone)
		if err != nil {
			return wrapError(err)
		}
	}

	err = c.primaryIndex.Insert(ctx, *clone)
	if err != nil {
		wrapError(err)
	}

	err = c.orderIndex.Update(ctx, clone)
	if err != nil {
		wrapError(err)
	}

	return nil
}

// TODO: If this fails, clean up
func (c *Collection) Create(s *types.Schema) error {
	var op errors.Op = "(*Collection).Create"

	_, err := c.storage.Write(nil)
	if err != nil {
		return errors.Wrap(op, errors.EIO, err)
	}

	c.Schema = s

	err = c.primaryIndex.Create()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	err = c.orderIndex.Save()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	err = c.Save()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	return nil
}

func (c *Collection) Load() error {
	var op errors.Op = "(*Collection).Load"
	err := c.primaryIndex.Load()

	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	err = c.orderIndex.Load()
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

	return nil
}

// Delete record with key `k`. An error is returned of no
// such record exists
func (c *Collection) Delete(ctx context.Context, k string) error {
	var op errors.Op = "(*Collection).Delete"

	err := c.primaryIndex.Delete(ctx, k)
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	err = c.primaryIndex.Save()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	return err
}

func (c *Collection) Save() error {
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

func (c *Collection) Info() {
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

func (c *Collection) Exists() bool {
	if ok, err := c.storage.Exists(); !ok || err != nil {
		return false
	}

	return true

}
