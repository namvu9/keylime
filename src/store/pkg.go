/*
Package store implements a key-value store backed
by a B-tree.

*/
package store

import (
	"encoding/gob"
	"fmt"
	"io"
	"path"
)

func init() {
	fmt.Println("INIT")
	gob.Register(MockReadWriterTo{})
}

type ReadWriterTo interface {
	io.ReadWriter
	WithSegment(pathSegment string) ReadWriterTo
	Delete() error
	Exists() (bool, error)
}

type MockReadWriterTo struct {
	root     *MockReadWriterTo
	location string
	writes   map[string]bool
	deletes  map[string]bool
	reads    map[string]bool
}

func (rwt *MockReadWriterTo) Write(src []byte) (int, error) {
	rwt.root.writes[rwt.location] = true

	return 0, nil
}

func (rwt *MockReadWriterTo) Read(dst []byte) (int, error) {
	rwt.root.reads[rwt.location] = true
	return 0, nil
}

func (rwt *MockReadWriterTo) Delete() error {
	rwt.root.deletes[rwt.location] = true
	return nil
}

func (rwt *MockReadWriterTo) Exists() (bool, error) {
	return true, nil
}

func (mrwt *MockReadWriterTo) WithSegment(s string) ReadWriterTo {
	rwt := &MockReadWriterTo{
		root:     mrwt.root,
		location: path.Join(mrwt.location, s),
	}
	return rwt
}

func newMockReadWriterTo() *MockReadWriterTo {
	mrw := &MockReadWriterTo{
		writes:  make(map[string]bool),
		reads:   make(map[string]bool),
		deletes: make(map[string]bool),
	}
	mrw.root = mrw

	return mrw
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
			err := c.Save()
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

func (s Store) save() error {
	for _, c := range s.collections {
		err := c.Save()
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) hasCollection(name string) bool {
	if ok, err := s.storage.WithSegment(name).Exists(); !ok || err != nil {
		return false
	}

	return true
}
