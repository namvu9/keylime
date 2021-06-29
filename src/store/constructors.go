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
		os.Stderr.WriteString("Warning: Storage has not been initialized\n")
		s.storage = newIOReporter()
	}

	return s
}

func newCollection(name string, s ReadWriterTo) *Collection {
	c := &Collection{
		Name:    name,
		storage: newIOReporter(),
	}

	if s != nil {
		c.storage = s.WithSegment(name)
		c.primaryIndex =
			newKeyIndex(2, s.WithSegment(name))
	} else {
		c.primaryIndex = newKeyIndex(2, c.storage)
	}

	return c
}

func newKeyIndex(t int, s ReadWriterTo) *KeyIndex {
	ki := &KeyIndex{
		T:       t,
		storage: newIOReporter(),
	}

	if s != nil {
		ki.storage = s.WithSegment("key_index")
	}

	ki.bufWriter = newWriteBuffer(s)

	ki.root = ki.newPage(true)
	ki.RootPage = ki.root.ID

	return ki
}

func (ki *KeyIndex) newPage(leaf bool) *Page {
	p := newPage(ki.T, leaf, ki.bufWriter)
	return p
}

func newPage(t int, leaf bool, bs *WriteBuffer) *Page {
	id := uuid.New().String()
	mockBs := newWriteBuffer(nil)

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

func newPageWithID(t int, id string, bs *WriteBuffer) *Page {
	mockBs := newWriteBuffer(nil)

	p := &Page{
		ID:     id,
		leaf:   false,
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
