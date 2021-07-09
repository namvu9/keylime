package store

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/namvu9/keylime/src/errors"
	"github.com/namvu9/keylime/src/index"
	"github.com/namvu9/keylime/src/repository"
	"github.com/namvu9/keylime/src/types"
)

//A Collection is a named container for a group of records
type Collection struct {
	Schema types.Schema
	Name   string
	Index  index.Index
	Blocks Blocklist

	repo   repository.Repository
}

func (c *Collection) ID() string {
	return c.Name
}

// Get the value associated with the key `k`, if a record
// with that key exists. Otherwise, nil is returned
func (c *Collection) Get(ctx context.Context, k string) (*types.Document, error) {
	ref, err := c.Index.Get(ctx, k)
	if err != nil {
		return nil, err
	}

	block, err := c.Blocks.Get(ID(ref.Value))
	if err != nil {
		return nil, err
	}

	doc, err := block.Get(k)
	if err != nil {
		return nil, err
	}

	fullDoc := c.Schema.WithDefaults(*doc)
	return &fullDoc, nil
}

type Fields = map[string]interface{}

// Set the value associated with key `k` in collection `c`.
// If a record with that key already exists in the
// collection, an error is returned.
func (c *Collection) Set(ctx context.Context, k string, fields Fields) error {
	log.Printf("Setting %s = %v in %s\n", k, fields, c.ID())
	wrapError := errors.WrapWith("(*Collection).Set", errors.EInternal)
	doc := types.NewDoc(k).Set(fields)

	err := c.Schema.Validate(doc)
	if err != nil {
		return wrapError(err)
	}

	blockID, err := c.Blocks.insert(ctx, doc)
	if err != nil {
		return err
	}

	if err := c.Index.Insert(ctx, k, blockID); err != nil {
		return err
	}

	err = c.repo.Flush()
	if err != nil {
		return err
	}

	log.Printf("Done setting %s in %s\n", k, c.ID())
	return nil
}

func (c *Collection) commit() error {
	err := c.repo.Save(c)
	if err != nil {
		return err
	}

	return c.repo.Flush()
}

func (c *Collection) GetLast(ctx context.Context, n int) ([]types.Document, error) {
	return c.Blocks.GetN(n, false), nil
}

func (c *Collection) GetFirst(ctx context.Context, n int) ([]types.Document, error) {
	// TODO: return error from oi
	return c.Blocks.GetN(n, true), nil
}

func (c *Collection) Update(ctx context.Context, k string, fields map[string]interface{}) error {
	// Retrieve record
	wrapError := errors.WrapWith("(*Collection).Update", errors.EInternal)
	ref, err := c.Index.Get(ctx, k)
	if err != nil {
		return wrapError(err)
	}

	block, err := c.Blocks.Get(ID(ref.Value))
	if err != nil {
		return err
	}

	doc, err := block.Get(k)
	if err != nil {
		return err
	}

	err = block.Update(doc.Update(fields))
	if err != nil {
		return wrapError(err)
	}

	err = c.repo.Save(block)
	if err != nil {
		wrapError(err)
	}

	return c.commit()
}

// TODO: If this fails, clean up
func (c *Collection) Create(ctx context.Context, s *types.Schema) error {
	log.Printf("Creating collection %s\n", c.ID())
	var op errors.Op = "(*Collection).Create"

	if s != nil {
		c.Schema = *s
	}

	c.Blocks = newBlocklist(200, c.repo)
	err := c.Blocks.create()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	c.Index = index.New(50, c.repo)
	err = c.Index.Create()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	err = c.repo.Save(c)
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	err = c.repo.Flush()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	log.Printf("Done creating collection %s\n", c.ID())
	return nil
}

// Delete record with key `k`. An error is returned of no
// such record exists
func (c *Collection) Delete(ctx context.Context, k string) error {
	var op errors.Op = "(*Collection).Delete"

	err := c.Index.Delete(ctx, k)
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	err = c.Index.Save()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	err = c.Blocks.remove(ctx, k)
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	err = c.Blocks.save()
	if err != nil {
		return errors.Wrap(op, errors.EInternal, err)
	}

	return c.commit()
}

func (c *Collection) Info(ctx context.Context) string {
	if ok, err := c.repo.Exists(c.ID()); !ok && err == nil {
		return fmt.Sprintf("Collection %s does not exist", c.ID())
	} else if err != nil {
		return fmt.Sprintf("Error: %s", err)
	}

	var sb strings.Builder
	sb.WriteString("\n---------------\n")
	sb.WriteString(fmt.Sprintf("Collection: %s\n", c.ID()))
	sb.WriteString("---------------\n")

	sb.WriteString(c.Schema.String())
	sb.WriteString("\n")
	sb.WriteString(c.Index.Info())
	sb.WriteString("\n")
	sb.WriteString(c.Blocks.Info())
	sb.WriteString("\n")

	return sb.String()
}

func (c *Collection) load() error {
	c.Index.SetRepo(c.repo)
	c.Blocks.repo = repository.WithFactory(c.repo, &BlockFactory{200, c.repo})
	return nil
}

func newCollection(name string, r repository.Repository) *Collection {
	c := &Collection{
		Name: name,
		repo: repository.WithScope(r, name),
	}

	return c
}
