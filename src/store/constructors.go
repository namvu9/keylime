package store

import (
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
		os.Stderr.WriteString("Warning: Storage has not been initialized")
		s.storage = &MockReadWriterTo{}
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
		c.primaryIndex = newKeyIndex(2000, nil)
	}

	return c
}

func newKeyIndex(t int, s ReadWriterTo) *KeyIndex {
	ki := &KeyIndex{
		T:         t,
		storage:   newMockReadWriterTo(),
	}

	if s != nil {
		ki.storage = s.WithSegment("key_index")
	}

	ki.bufWriter = newBufferedStorage(s)

	ki.root = ki.newPage(true)

	return ki
}

type BufferedStorage struct {
	ReadWriterTo
	writeBuf  map[string]*page
	deleteBuf map[string]*page
}

// Write schedules a page for being written to disk. If a
// page has already been scheduled for a write or delete,
// Write is a no-op.
func (bs *BufferedStorage) Write(p *page) error {
	if _, ok := bs.deleteBuf[p.ID]; !ok {
		bs.writeBuf[p.ID] = p
	}
	return nil
}

func (bs *BufferedStorage) Delete(p *page) error {
	bs.deleteBuf[p.ID] = p
	delete(bs.writeBuf, p.ID)
	return nil
}

func (bs *BufferedStorage) flush() error {
	for k, _ := range bs.writeBuf {
		// TODO: Encode
		bs.WithSegment(k).Write(nil)
		delete(bs.writeBuf, k)
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
		make(map[string]*page),
		make(map[string]*page),
	}

	if rw != nil {
		bs.ReadWriterTo = rw
	}

	return bs
}

func (ki *KeyIndex) newPage(leaf bool) *page {
	p := newPage(ki.T, leaf, ki.bufWriter)
	return p
}

func newPage(t int, leaf bool, bs *BufferedStorage) *page {
	id := uuid.New().String()
	mockBs := newBufferedStorage(nil)

	p := &page{
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
