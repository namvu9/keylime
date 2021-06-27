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
	baseDir  string

	writeBuf map[*page]bool
	c        *Collection
	root     *page

	storage PageReadWriter
}

type PageReadWriter interface {
	Write(*page) error
	Read(*page) error
}

func (ki *KeyIndex) Insert(ctx context.Context, r record.Record) error {
	if ki.root.Full() {
		newRoot := ki.newPage()
		newRoot.children = []*page{ki.root}
		newRoot.splitChild(0)

		ki.RootPage = newRoot.ID
		ki.root = newRoot
		ki.Height++

		newRoot.save()
		ki.c.Save()
	}

	page := ki.root.iter(byKey(r.Key)).forEach(splitFullPage).Get()
	page.insert(r.Key, r.Value)

	return nil
}

func (ki *KeyIndex) loadPage(p *page) error {
	err := ki.storage.Read(p)
	if err != nil {
		return err
	}

	if !p.leaf {
		for _, child := range p.children {
			child.ki = p.ki
		}
	}
	fmt.Println("Loaded page", p.ID)
	return nil
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

		//var buf bytes.Buffer
		//enc := gob.NewEncoder(&buf)
		//err := enc.Encode(p)
		//if err != nil {
		//return err
		//}

		//err = os.WriteFile(path.Join(ki.baseDir, p.ID), buf.Bytes(), 0755)
		err := ki.storage.Write(p)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ki *KeyIndex) writePage(p *page) {
	if ki == nil {
		return
	}

	ki.writeBuf[p] = true
}

// TODO: IMPLEMENT
func (ki *KeyIndex) Save() error {
	return nil
}

func (ki *KeyIndex) newPage() *page {
	p := newPage(ki.T)
	p.ki = ki
	return p
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
		return nil, nil
	}

	return &node.records[i], nil
}

func newKeyIndex(t int) *KeyIndex {
	ki := &KeyIndex{
		T:        t,
		writeBuf: make(map[*page]bool),
		storage:  MockPageStorage{},
	}
	ki.root = ki.newPage()
	ki.root.leaf = true
	return ki
}

type MockPageStorage struct{}

func (mps MockPageStorage) Write(p *page) error {
	return nil
}

func (mps MockPageStorage) Read(p *page) error {
	p.loaded = true
	return nil
}
