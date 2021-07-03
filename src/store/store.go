package store

import (
	"context"
	"io"
	"io/ioutil"

	"github.com/namvu9/keylime/src/types"
)

type Store struct {
	baseDir     string
	t           int
	collections map[string]*collection

	storage ReadWriterTo
}

func (s Store) Collection(name string) types.Collection {
	//var op errors.Op = "(Store).Collection"

	c, ok := s.collections[name]
	if !ok {
		c := newCollection(name, s.storage)
		s.collections[name] = c
		return c
	}

	return c
}

func (s *Store) Info(ctx context.Context) {
	files, _ := ioutil.ReadDir(s.baseDir)
	for _, f := range files {
		if f.IsDir() {
			s.Collection(f.Name()).Info(ctx)
		}
	}
}

type ReadWriterTo interface {
	io.ReadWriter
	WithSegment(pathSegment string) ReadWriterTo
	Delete() error
	Exists() (bool, error)
}
