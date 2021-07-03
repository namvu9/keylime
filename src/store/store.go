package store

import (
	"io"

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
