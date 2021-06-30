/*
Package store implements a key-value store backed
by a B-tree.

*/
package store

import (
	"io"
	"io/ioutil"
)

type ReadWriterTo interface {
	io.ReadWriter
	WithSegment(pathSegment string) ReadWriterTo
	Delete() error
	Exists() (bool, error)
}

type Store struct {
	initialized bool
	baseDir     string
	t           int
	collections map[string]*Collection

	storage ReadWriterTo
}

func (s Store) Collection(name string) (*Collection, error) {
	//var op errors.Op = "(Store).Collection"

	c, ok := s.collections[name]
	if !ok {
		c := newCollection(name, s.storage)
		s.collections[name] = c
		return c, nil
	}

	return c, nil
}

type Option func(*Store)

func WithStorage(rw ReadWriterTo) Option {
	return func(s *Store) {
		s.storage = rw
	}
}

func (s *Store) hasCollection(name string) bool {
	if ok, err := s.storage.WithSegment(name).Exists(); !ok || err != nil {
		return false
	}

	return true
}

func (s *Store) Info() {
	files, _ := ioutil.ReadDir(s.baseDir)
	for _, f := range files {
		if f.IsDir() {
			c, _ := s.Collection(f.Name())
			c.Info()
		}
	}
}
