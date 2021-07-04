package store

import (
	"io"
	"os"

	"github.com/namvu9/keylime/src/types"
)

type Store struct {
	baseDir     string
	t           int
	collections map[string]*collection
	storage     ReadWriterTo
}

func (s Store) Collection(name string) types.Collection {
	c, ok := s.collections[name]
	if !ok {
		c := newCollection(name, s.storage)
		s.collections[name] = c
		return c
	}

	return c
}

type ReadWriterTo interface {
	io.ReadWriter
	WithSegment(pathSegment string) ReadWriterTo
	Delete() error
	Exists() (bool, error)
}

// New instantiates a store with the provided config and
// options
func New(cfg *Config, opts ...Option) *Store {
	s := &Store{
		baseDir:     cfg.BaseDir,
		t:           cfg.T,
		collections: make(map[string]*collection),
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

