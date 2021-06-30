package store

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"

	"github.com/namvu9/keylime/src/errors"
	"github.com/namvu9/keylime/src/types"
	klTypes "github.com/namvu9/keylime/src/types"
	record "github.com/namvu9/keylime/src/types"
)

// A Collection is a named container for a group of records
type Collection struct {
	Name   string
	Schema *klTypes.Schema

	primaryIndex *KeyIndex
	storage      ReadWriterTo
}

// Get the value associated with the key `k`, if a record
// with that key exists. Otherwise, nil is returned
func (c *Collection) Get(ctx context.Context, k string) ([]byte, error) {
	r, err := c.primaryIndex.Get(ctx, k)
	if err != nil {
		return nil, err
	}

	return r.Value, err
}

// Set the value associated with key `k` in collection `c`.
// If a record with that key already exists in the
// collection, an error is returned.
func (c *Collection) Set(ctx context.Context, k string, fields map[string]interface{}) error {
	wrapError := errors.WrapWith("(*Collection).Set", errors.InternalError)
	r := record.NewRecord(k)
	r.SetFields(fields)

	if c.Schema != nil {
		err := c.Schema.Validate(r)
		if err != nil {
			return wrapError(err)
		}
	}

	if err := c.primaryIndex.Insert(ctx, *r); err != nil {
		return err
	}

	if err := c.primaryIndex.Save(); err != nil {
		return fmt.Errorf("Could not persist primary index: %w", err)
	}

	return nil
}

// TODO: If this fails, clean up
func (c *Collection) Create(s *klTypes.Schema) error {
	var op errors.Op = "(*Collection).Create"

	_, err := c.storage.Write(nil)
	if err != nil {
		return errors.Wrap(op, errors.IOError, err)
	}

	c.Schema = s

	err = c.primaryIndex.Create()
	if err != nil {
		return errors.Wrap(op, errors.InternalError, err)
	}

	err = c.Save()
	if err != nil {
		return errors.Wrap(op, errors.InternalError, err)
	}

	return nil
}

func (c *Collection) Load() error {
	var op errors.Op = "(*Collection).Load"
	err := c.primaryIndex.Load()

	if err != nil {
		return errors.Wrap(op, errors.InternalError, err)
	}

	schemaReader := c.storage.WithSegment("schema")
	ok, err := schemaReader.Exists()
	if err != nil {
		return errors.Wrap(op, errors.IOError, err)
	}
	if ok {
		data, err := io.ReadAll(schemaReader)
		if err != nil {
			return errors.Wrap(op, errors.IOError, err)
		}

		s := types.NewSchema()

		buf := bytes.NewBuffer(data)
		dec := gob.NewDecoder(buf)
		err = dec.Decode(&s)
		if err != nil {
			return errors.Wrap(op, errors.IOError, err)
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
		return errors.Wrap(op, errors.InternalError, err)
	}

	err = c.primaryIndex.Save()
	if err != nil {
		return errors.Wrap(op, errors.InternalError, err)
	}

	return err
}

func (c *Collection) Save() error {
	wrapError := errors.WrapWith("(*Collection).Save", errors.IOError)

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
}

func (c *Collection) Exists() bool {
	if ok, err := c.storage.Exists(); !ok || err != nil {
		return false
	}

	return true

}
