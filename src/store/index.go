package store

import (
	"context"
	"fmt"

	"github.com/namvu9/keylime/src/record"
)

// KeyIndex represents a B-tree that indexes records by key
type KeyIndex struct {
	RootPage string
	Height   int
	T        int

	writeBuf map[*page]bool
	root     *page
	storage  ReadWriterTo
}

func (ki *KeyIndex) Insert(ctx context.Context, r record.Record) error {
	if ki.root.Full() {
		newRoot := ki.newPage(false)
		newRoot.children = []*page{ki.root}
		newRoot.splitChild(0)

		ki.RootPage = newRoot.ID
		ki.root = newRoot
		ki.Height++

		newRoot.save()
		ki.Save()
	}

	page := ki.root.iter(byKey(r.Key)).forEach(splitFullPage).Get()
	page.insert(r)

	return nil
}

func (ki *KeyIndex) Delete(ctx context.Context, key string) error {
	page := ki.root.iter(byKey(key)).forEach(handleSparsePage).Get()

	if err := page.Delete(key); err != nil {
		return err
	}

	if err := ki.flushWriteBuffer(); err != nil {
		return err
	}

	if ki.root.Empty() && !ki.root.Leaf() {
		ki.root = ki.root.children[0]
		ki.RootPage = ki.root.ID
		ki.Height--
		return ki.Save()
	}

	return nil
}

func (ki *KeyIndex) Get(ctx context.Context, key string) (*record.Record, error) {
	node := ki.root.iter(byKey(key)).Get()
	i, ok := node.keyIndex(key)
	if !ok {
		return nil, fmt.Errorf("KeyNotFound")
	}

	return &node.records[i], nil
}

func (ki *KeyIndex) flushWriteBuffer() error {
	defer func() {
		for p := range ki.writeBuf {
			delete(ki.writeBuf, p)
		}
	}()

	for p := range ki.writeBuf {
		if ki.storage == nil {
			return fmt.Errorf("Writing to nil PageReadWriter")
		}
		fmt.Println(p)
	}

	return nil
}

// OrderIndex indexes records by their order with respect to
// some attribute
type OrderIndex struct {
	head interface{}
	tail interface{}
}

func (oi *OrderIndex) Insert(r *record.Record) error {
	return nil
}

func (oi *OrderIndex) Delete(r *record.Record) error {
	return nil
}

func (c *Collection) OrderBy(attr interface{}) *OrderIndex {
	return &OrderIndex{}
}

func (ki *KeyIndex) Save() error { return nil }
func (ki *KeyIndex) Read() error { return nil }
