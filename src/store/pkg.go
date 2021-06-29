/*
Package store implements a key-value store backed
by a B-tree.

*/
package store

import (
	"io"
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
	c, ok := s.collections[name]
	if !ok {
		c = newCollection(name, s.storage)

		if s.hasCollection(name) {
			err := c.Load()
			if err != nil {
				return nil, err
			}

			s.collections[name] = c
		} else {
			err := c.Create()
			if err != nil {
				return nil, err
			}
		}

		s.collections[name] = c
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
