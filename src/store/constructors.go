package store

import (
	"bytes"
	"encoding/gob"
	"os"

	"github.com/google/uuid"
)

// New instantiates a store with the provided config and
// options
func New(cfg *Config, opts ...Option) *Store {
	s := &Store{
		baseDir:     cfg.BaseDir,
		t:           cfg.T,
		collections: make(map[string]*Collection),
	}

	for _, opt := range opts {
		opt(s)
	}

	if s.storage == nil {
		os.Stderr.WriteString("Warning: Storage has not been initialized\n")
		s.storage = newMockReadWriterTo()
	}

	return s
}

func newCollection(name string, s ReadWriterTo) *Collection {
	c := &Collection{
		Name:    name,
		storage: newMockReadWriterTo(),
	}

	if s != nil {
		c.storage = s.WithSegment(name)
		c.primaryIndex =
			newKeyIndex(2000, s.WithSegment(name))
	} else {
		c.primaryIndex = newKeyIndex(2000, c.storage)
	}

	return c
}

func newKeyIndex(t int, s ReadWriterTo) *KeyIndex {
	ki := &KeyIndex{
		T:       t,
		storage: newMockReadWriterTo(),
	}

	if s != nil {
		ki.storage = s.WithSegment("key_index")
	}

	ki.bufWriter = newBufferedStorage(s)

	ki.root = ki.newPage(true)
	ki.RootPage = ki.root.ID

	return ki
}

type BufferedStorage struct {
	ReadWriterTo
	writeBuf  map[string]*Page
	deleteBuf map[string]*Page
}

// Write schedules a page for being written to disk. If a
// page has already been scheduled for a write or delete,
// Write is a no-op.
func (bs *BufferedStorage) Write(p *Page) error {
	if _, ok := bs.deleteBuf[p.ID]; !ok {
		bs.writeBuf[p.ID] = p
	}
	return nil
}

func (bs *BufferedStorage) Delete(p *Page) error {
	bs.deleteBuf[p.ID] = p
	delete(bs.writeBuf, p.ID)
	return nil
}

func (bs *BufferedStorage) flush() error {
	for id, p := range bs.writeBuf {
		buf := new(bytes.Buffer)
		enc := gob.NewEncoder(buf)

		if err := enc.Encode(p.ToSerialized()); err != nil {
			return err
		}
		_, err := bs.WithSegment(id).Write(buf.Bytes())
		delete(bs.writeBuf, id)
		return err
	}

	for k := range bs.deleteBuf {
		bs.WithSegment(k).Delete()
		delete(bs.deleteBuf, k)
	}

	return nil
}

func newBufferedStorage(rw ReadWriterTo) *BufferedStorage {
	bs := &BufferedStorage{
		newMockReadWriterTo(),
		make(map[string]*Page),
		make(map[string]*Page),
	}

	if rw != nil {
		bs.ReadWriterTo = rw
	}

	return bs
}

func (ki *KeyIndex) newPage(leaf bool) *Page {
	p := newPage(ki.T, leaf, ki.bufWriter)
	return p
}

func newPage(t int, leaf bool, bs *BufferedStorage) *Page {
	id := uuid.New().String()
	mockBs := newBufferedStorage(nil)

	p := &Page{
		ID:     id,
		leaf:   leaf,
		t:      t,
		writer: mockBs,
		reader: mockBs.WithSegment(id),
		loaded: true,
	}

	if bs != nil {
		p.writer = bs
		p.reader = bs.WithSegment(id)
	}

	return p
}

func newPageWithID(t int, leaf bool, id string, bs *BufferedStorage) *Page {
	mockBs := newBufferedStorage(nil)

	p := &Page{
		ID:     id,
		leaf:   leaf,
		t:      t,
		writer: mockBs,
		reader: mockBs.WithSegment(id),
		loaded: true,
	}

	if bs != nil {
		p.writer = bs
		p.reader = bs.WithSegment(id)
	}

	return p
}
