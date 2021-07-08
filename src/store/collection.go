package store

import (
	"context"
	"fmt"
	"sync"

	"github.com/namvu9/keylime/src/errors"
	"github.com/namvu9/keylime/src/repository"
	"github.com/namvu9/keylime/src/types"
)

//A collection is a named container for a group of records
type collection struct {
	Name   string
	Schema *types.Schema

	index  *Index
	blocks *Blocklist
	loaded bool
	repo   repository.Repository
}

func (c *collection) ID() string {
	return c.Name
}

// Get the value associated with the key `k`, if a record
// with that key exists. Otherwise, nil is returned
func (c *collection) Get(ctx context.Context, k string) (types.Document, error) {
	err := c.load()
	if err != nil {
		return types.NewDoc(k), err
	}

	r, err := c.index.get(ctx, k)
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

	if old, err := c.index.insert(ctx, doc); err != nil {
		return err
	} else if old != nil {
		return c.blocks.update(ctx, doc)
	}

	return c.blocks.insert(ctx, doc)
}

func (c *collection) commit() error {
	errChan := make(chan error, 2)
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := c.index.save(); err != nil {
			errChan <- err
			return
		}

		//if err := c.index.bufWriter.flush(); err != nil {
		//errChan <- err
		//}
	}()

	go func() {
		defer wg.Done()
		if err := c.blocks.save(); err != nil {
			errChan <- err
			return
		}

		if err := c.blocks.repo.Flush(); err != nil {
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
	return c.blocks.Get(n, false), nil
}

func (c *collection) GetFirst(ctx context.Context, n int) ([]types.Document, error) {
	err := c.load()
	if err != nil {
		return []types.Document{}, err
	}

	// TODO: return error from oi
	return c.blocks.Get(n, true), nil
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
	r, err := c.index.get(ctx, k)
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

	_, err = c.index.insert(ctx, clone)
	if err != nil {
		wrapError(err)
	}

	err = c.blocks.update(ctx, clone)
	if err != nil {
		wrapError(err)
	}

	return nil
}

// TODO: If this fails, clean up
func (c *collection) Create(ctx context.Context, s *types.Schema) error {
	var op errors.Op = "(*Collection).Create"

	//_, err := c.storage.Write(nil)
	//if err != nil {
	//return errors.Wrap(op, errors.EIO, err)
	//}

	c.Schema = s

	err := c.index.create()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	c.blocks = newOrderIndex(200, c.repo)
	err = c.blocks.create()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	err = c.save()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	fmt.Println("CREATED collection", c.blocks.Head)

	return nil
}

func (c *collection) load() error {
	if c.loaded {
		return nil
	}

	//var op errors.Op = "(*Collection).Load"
	//c.PrimaryIndex.storage = c.storage
	//c.PrimaryIndex.bufWriter = newWriteBuffer(c.storage)
	//fmt.Println("C", c.PrimaryIndex.RootPage)
	//err := c.PrimaryIndex.Load()
	//if err != nil {
	//return errors.Wrap(op, errors.EInternal, err)
	//}

	c.blocks.repo = c.repo
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

	err := c.index.remove(ctx, k)
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	err = c.index.save()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	err = c.blocks.remove(ctx, k)
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	err = c.blocks.save()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	return err
}

func (c *collection) save() error {
	wrapError := errors.WrapWith("(*Collection).Save", errors.EIO)

	err := c.repo.SaveCommit(c)
	if err != nil {
		return wrapError(err)
	}

	fmt.Println("SAVED COLLECTION")

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
	c.index.Info()
	fmt.Println()
	c.blocks.Info()
	fmt.Println()
}

func (c *collection) exists() bool {
	//if ok, err := c.storage.Exists(); !ok || err != nil {
	//return false
	//}

	return true
}

func newCollection(name string, r repository.Repository) *collection {
	//t := 50
	c := &collection{
		Name:    name,
		repo:    repository.WithScope(r, name),
	}

	return c
}
