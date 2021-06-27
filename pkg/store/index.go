package store

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"os"
	"path"

	"github.com/namvu9/keylime/pkg/record"
)

//type Index interface {
//Insert(context.Context, record.Record) error
//Delete(context.Context, string) error
//Update(context.Context, record.Record) error
//Get(context.Context, string) error

//Save() error
//}

// KeyIndex represents a B-tree that indexes records by key
type KeyIndex struct {
	RootPage string
	Height   int
	T        int
	baseDir  string

	s        *Store
	writeBuf map[*Page]bool
	c        *Collection
	root     *Page
}

func (ki *KeyIndex) Insert(ctx context.Context, r record.Record) error {
	if ki.root.Full() {
		s := ki.newPage()
		s.children = []*Page{ki.root}
		s.splitChild(0)

		ki.RootPage = s.ID
		ki.root = s
		ki.Height++


		s.save()
		ki.c.save()
	}

	page := ki.root.iter(byKey(r.Key)).forEach(splitFullPage).Get()
	page.insert(r.Key, r.Value)

	return nil
}

func (ki *KeyIndex) loadPage(p *Page) error {
	if ki == nil {
		return fmt.Errorf("Page %s has no reference to parent collection", p.ID)
	}

	data, err := ioutil.ReadFile(path.Join(ki.baseDir, p.ID))
	if err != nil {
		return err
	}

	dec := gob.NewDecoder(bytes.NewBuffer(data))
	err = dec.Decode(p)
	if err != nil {
		return err
	}

	p.loaded = true

	if !p.leaf {
		for _, child := range p.children {
			child.ki = p.ki
		}
	}
	fmt.Println("Loaded page", p.ID)
	return nil
}

func (ki *KeyIndex) FlushWriteBuffer() error {
	defer func() {
		for p := range ki.writeBuf {
			delete(ki.writeBuf, p)
		}
	}()

	for p := range ki.writeBuf {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err := enc.Encode(p)
		if err != nil {
			return err
		}

		err = os.WriteFile(path.Join(ki.baseDir, p.ID), buf.Bytes(), 0755)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ki *KeyIndex) writePage(p *Page) {
	if ki == nil {
		return
	}

	ki.writeBuf[p] = true
}

func (ki *KeyIndex) Save() error {
	return nil
}

func (ki *KeyIndex) newPage() *Page {
	p := newPage(ki.T)
	p.ki = ki
	return p
}

func (ki *KeyIndex) Delete(ctx context.Context, key string) error {
	page := ki.root.iter(byKey(key)).forEach(handleSparsePage).Get()

	if err := page.Delete(key); err != nil {
		return err
	}

	if err := ki.FlushWriteBuffer(); err != nil {
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
		return nil, nil
	}

	return &node.records[i], nil
}

func newKeyIndex(t int) *KeyIndex {
	ki := &KeyIndex{
		T:        t,
		writeBuf: make(map[*Page]bool),
	}
	ki.root = ki.newPage()
	ki.root.leaf = true
	return ki
}
