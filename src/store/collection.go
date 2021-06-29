package store

import (
	"context"
	"fmt"

	"github.com/namvu9/keylime/src/record"
)

// A Collection is a named container for a group of records
type Collection struct {
	Name    string
	baseDir string

	primaryIndex *KeyIndex
	storage      ReadWriterTo
}

// Get the value associated with the key `k`, if a record
// with that key exists. Otherwise, nil is returned
func (c *Collection) Get(ctx context.Context, k string) []byte {
	r, err := c.primaryIndex.Get(ctx, k)
	if err != nil {
		return nil
	}

	return r.Value
}

// Set the value associated with key `k` in collection `c`.
// If a record with that key already exists in the
// collection, an error is returned.
func (c *Collection) Set(ctx context.Context, k string, value []byte) error {
	r := record.New(k, value)

	if err := c.primaryIndex.Insert(ctx, r); err != nil {
		return err
	}

	if err := c.primaryIndex.Save(); err != nil {
		return fmt.Errorf("Could not persist primary index: %w", err)
	}

	return nil
}

func (c *Collection) Save() error {
	_, err := c.storage.Write(nil)
	if err != nil {
		return err
	}
	err = c.primaryIndex.Save()
	if err != nil {
		return err
	}

	return nil
}

func (c *Collection) Load() error {
	err := c.primaryIndex.Load()
	return err
}

// Delete record with key `k`. An error is returned of no
// such record exists
func (c *Collection) Delete(ctx context.Context, k string) error {
	err := c.primaryIndex.Delete(ctx, k)
	if err != nil {
		return err
	}

	err = c.primaryIndex.Save()
	if err != nil {
		return err
	}

	return err
}

