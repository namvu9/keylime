/*
Package store implements a key-value store backed
by a B-tree.

*/
package store

import (
	"fmt"
	"io"
	"os"
	"path"
)

type ReadWriterTo interface {
	io.ReadWriter
	WithSegment(pathSegment string) ReadWriterTo
}

type MockReadWriterTo struct {
	location string
}

func (lrw MockReadWriterTo) Write(src []byte) (int, error) {
	return 0, nil
}

func (lrw MockReadWriterTo) Read(dst []byte) (int, error) {
	return 0, nil
}

func (lrw MockReadWriterTo) WithSegment(s string) ReadWriterTo {
	return MockReadWriterTo{
		location: path.Join(lrw.location, s),
	}
}

type Store struct {
	initialized bool
	baseDir     string
	t           int
	collections map[string]*Collection

	storage ReadWriterTo
}

func (s Store) Collection(name string) (*Collection, error) {
	if !s.initialized {
		return nil, fmt.Errorf("Store has not been initialized")
	}

	c, ok := s.collections[name]
	if !ok {
		c = newCollection(name)
		c.storage = s.storage.WithSegment(name)

		if s.hasCollection(name) {
			err := c.Load()
			if err != nil {
				return nil, err
			}

			s.collections[name] = c
		} else {
			err := c.Save()
			if err != nil {
				return nil, err
			}
		}

		s.collections[name] = c
	}

	return c, nil
}

// Collections returns a list of collections in a store
func (s Store) Collections() []*Collection {
	var out []*Collection
	for _, c := range s.collections {
		out = append(out, c)
	}

	return out
}

// New instantiates a store with the provided config and
// options
func New(cfg *Config) *Store {
	s := &Store{
		baseDir:     cfg.BaseDir,
		t:           cfg.T,
		collections: make(map[string]*Collection),
		storage:     cfg.Storage,
	}

	return s
}

func (s Store) createCollection(name string) (*Collection, error) {
	c := newCollection(name)
	err := c.Save()

	return c, err
}

func (s Store) save() error {
	for _, c := range s.collections {
		err := c.Save()
		if err != nil {
			return err
		}
	}

	// TODO: Save store

	return nil
}

func (s *Store) hasCollection(name string) bool {
	if _, err := os.Stat(path.Join(s.baseDir, name)); os.IsNotExist(err) {
		return false
	}

	return true
}
