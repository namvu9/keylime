package store

import (
	"bytes"
	"context"
	"encoding/gob"
	"io"

	"github.com/namvu9/keylime/src/errors"
	"github.com/namvu9/keylime/src/record"
)

// KeyIndex represents a B-tree that indexes records by key
type KeyIndex struct {
	RootPage string
	Height   int
	T        int

	root      *Page
	bufWriter *BufferedStorage
	storage   ReadWriterTo
}

func (ki *KeyIndex) Insert(ctx context.Context, r record.Record) error {
	if ki.root.Full() {
		newRoot := ki.newPage(false)
		newRoot.children = []*Page{ki.root}
		newRoot.splitChild(0)

		ki.RootPage = newRoot.ID
		ki.root = newRoot
		ki.Height++

		newRoot.save()
		ki.Save()
	}

	page := ki.root.iter(byKey(r.Key)).forEach(splitFullPage).Get()
	page.insert(r)

	return ki.bufWriter.flush()
}

func (ki *KeyIndex) Delete(ctx context.Context, key string) error {
	const op errors.Op = "(*KeyIndex).Delete"

	page := ki.root.iter(byKey(key)).forEach(handleSparsePage).Get()

	if err := page.Delete(key); err != nil {
		return errors.Wrap(op, errors.InternalError, err)
	}

	if ki.root.Empty() && !ki.root.Leaf() {
		oldRoot := ki.root
		ki.root = ki.root.children[0]
		ki.RootPage = ki.root.ID
		ki.Height--

		oldRoot.deletePage()
		ki.Save()
	}

	return ki.bufWriter.flush()
}

func (ki *KeyIndex) Get(ctx context.Context, key string) (*record.Record, error) {
	const op errors.Op = "(*KeyIndex).Get"

	node := ki.root.iter(byKey(key)).Get()
	i, ok := node.keyIndex(key)
	if !ok {
		return nil, errors.NewKeyNotFoundError(op, key)
	}

	return &node.records[i], nil
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

type KeyIndexSerialized struct {
	RootPage string
	T        int
	Height   int
}

func (ki *KeyIndex) Save() error {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	enc.Encode(ki)

	_, err := ki.storage.Write(buf.Bytes())
	if err != nil {
		return err
	}

	return ki.root.save()
}
func (ki *KeyIndex) read() error {
	buf := new(bytes.Buffer)

	for {
		var b = make([]byte, 100)
		n, err := ki.storage.Read(b)
		if err != nil && err != io.EOF {
			return err
		}

		if n > 0 {
			buf.Write(b[:n])
		}

		if err == io.EOF {
			dec := gob.NewDecoder(buf)
			err := dec.Decode(ki)
			if err != nil {
				return err
			}

			return nil
		}
	}
}

func (ki *KeyIndex) Load() error {
	err := ki.read()
	if err != nil {
		return err
	}

	return ki.loadRoot()
}

func (ki *KeyIndex) loadRoot() error {
	ki.root = newPageWithID(ki.T, false, ki.RootPage, ki.bufWriter)

	return ki.root.load()
}
