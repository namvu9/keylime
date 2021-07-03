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
	Name      string
	HasSchema bool
	Schema    *types.Schema

	primaryIndex *KeyIndex
	orderIndex   *OrderIndex
	storage      ReadWriterTo
}

// get the value associated with the key `k`, if a record
// with that key exists. Otherwise, nil is returned
func (c *Collection) get(ctx context.Context, k string) (*types.Record, error) {
	r, err := c.primaryIndex.get(ctx, k)
	if err != nil {
		return nil, err
	}

	if c.Schema != nil {
		return c.Schema.WithDefaults(r), nil
	}

	return r, nil
}

type Fields = map[string]interface{}

// set the value associated with key `k` in collection `c`.
// If a record with that key already exists in the
// collection, an error is returned.
func (c *Collection) set(ctx context.Context, k string, fields Fields) error {
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
		if err := c.primaryIndex.insert(ctx, *r); err != nil {
			errChan <- err
			cancelFn()
			return
		}
	}()

	go func() {
		defer wg.Done()

		if err := c.orderIndex.insert(ctx, r); err != nil {
			errChan <- err
			return
		}

	}()

	wg.Wait()

	if len(errChan) != 0 {
		return <-errChan
	}

	return nil
}

func (c *Collection) commit() error {
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
			return
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
			return
		}
	}()

	wg.Wait()

	if len(errChan) != 0 {
		return <-errChan
	}

	return nil
}

func (c *Collection) getLast(ctx context.Context, n int) []*types.Record {
	return c.orderIndex.Get(n, false)
}

func (c *Collection) getFirst(ctx context.Context, n int) []*types.Record {
	return c.orderIndex.Get(n, true)
}

func (c *Collection) update(ctx context.Context, k string, fields map[string]interface{}) error {
	// Retrieve record
	wrapError := errors.WrapWith("(*Collection).Update", errors.EInternal)
	r, err := c.primaryIndex.get(ctx, k)
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

	err = c.primaryIndex.insert(ctx, *clone)
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
func (c *Collection) create(s *types.Schema) error {
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

	err = c.orderIndex.save()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	err = c.save()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	return nil
}

func (c *Collection) load() error {
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

	return nil
}

// remove record with key `k`. An error is returned of no
// such record exists
func (c *Collection) remove(ctx context.Context, k string) error {
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

func (c *Collection) save() error {
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

func (c *Collection) info() {
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

func (c *Collection) exists() bool {
	if ok, err := c.storage.Exists(); !ok || err != nil {
		return false
	}

	return true

}
