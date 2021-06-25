/*
Package store implements a key-value store backed
by a B-tree.

*/
package store

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
)

type Store struct {
	initialized bool
	baseDir     string
	t           int
	collections map[string]*Collection
}

func (s Store) Collection(name string) (*Collection, error) {
	if !s.initialized {
		return nil, fmt.Errorf("Store has not been initialized")
	}

	c, ok := s.collections[name]
	if !ok {
		if s.hasCollection(name) {
			c, err := s.loadCollection(name)
			if err != nil {
				return nil, err
			}

			s.collections[name] = c
			return c, err
		}

		c, err := s.createCollection(name)
		if err != nil {
			return nil, err
		}

		s.collections[name] = c
		return c, err
	}

	return c, nil
}

func (s Store) Collections() []*Collection {
	var out []*Collection
	for _, c := range s.collections {
		out = append(out, c)
	}

	return out
}

// New instantiates a store with the provided config and
// options
func New(cfg *Config, opts ...Option) *Store {
	s := &Store{
		baseDir:     cfg.BaseDir,
		t:           cfg.T,
		collections: make(map[string]*Collection),
	}

	for _, opt := range opts {
		opt.Apply(s)
	}

	return s
}

func (s *Store) Init() error {
	err := s.loadCollections()
	if err != nil {
		return err
	}

	s.initialized = true

	return nil
}

func (s Store) createCollection(name string) (*Collection, error) {
	root := newPage(s.t)
	root.leaf = true
	root.loaded = true

	c := &Collection{
		Name:     name,
		RootPage: root.ID,
		T:        s.t,
		root:     root,
		baseDir:  path.Join(s.baseDir, name),
		writeBuf: make(map[*Page]bool),
		s:        &s,
	}

	root.c = c

	err := os.Mkdir(path.Join(s.baseDir, c.Name), 0755)
	if err != nil {
		return c, err
	}

	c.writePage(root)
	err = c.flushWriteBuffer()
	if err != nil {
		return c, err
	}

	err = s.writeCollection(c)
	if err != nil {
		return c, err
	}

	return c, nil
}

func (s Store) writeCollection(c *Collection) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(c)
	if err != nil {
		return err
	}

	err = os.WriteFile(path.Join(s.baseDir, c.Name, "collection"), buf.Bytes(), 0755)
	if err != nil {
		return err
	}

	fmt.Println("Wrote collection", c.Name)

	return nil
}

func (s *Store) loadCollection(name string) (*Collection, error) {
	p := path.Join(s.baseDir, name, "collection")
	data, err := ioutil.ReadFile(p)
	if err != nil {
		return nil, err
	}

	// TODO: implement newCollection function
	c := Collection{baseDir: path.Join(s.baseDir, name), writeBuf: make(map[*Page]bool), s: s}

	dec := gob.NewDecoder(bytes.NewBuffer(data))
	err = dec.Decode(&c)
	if err != nil {
		return nil, err
	}

	c.root = c.newPage()
	c.root.ID = c.RootPage

	err = c.loadPage(c.root)
	if err != nil {
		return nil, err
	}

	fmt.Println(c.root)

	return &c, err
}

// NewCollection returns a store
func NewCollection(t int, opts ...CollectionOption) *Collection {
	tree := &Collection{
		T: t,
	}

	for _, fn := range opts {
		fn(tree)
	}

	if tree.root == nil {
		tree.root = tree.newPage()
		tree.root.leaf = true
	}

	return tree
}
func (s *Store) hasCollection(name string) bool {
	if _, err := os.Stat(path.Join(s.baseDir, name)); os.IsNotExist(err) {
		return false
	}

	return true
}

func (s *Store) loadCollections() error {
	files, err := ioutil.ReadDir(s.baseDir)
	if err != nil {
		log.Fatal("Could not load collections")
	}

	for _, file := range files {
		if file.IsDir() {
			c, err := s.loadCollection(file.Name())
			if err != nil {
				return err
			}
			s.collections[file.Name()] = c
		}
	}

	return nil
}
