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
		Name: name,
		s:    &s,
		primaryIndex: KeyIndex{
			writeBuf: make(map[*Page]bool),
			baseDir:  path.Join(s.baseDir, name),
			T:        s.t,
			root:     root,
			RootPage: root.ID,
		},
	}

	root.ki = &c.primaryIndex

	err := os.Mkdir(path.Join(s.baseDir, c.Name), 0755)
	if err != nil {
		return c, err
	}

	c.primaryIndex.writePage(root)
	c.primaryIndex.Save()
	//err = c.FlushWriteBuffer()
	//if err != nil {
	//return c, err
	//}

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
	fmt.Println(c)

	return nil
}

func (s *Store) loadCollection(name string) (*Collection, error) {
	p := path.Join(s.baseDir, name, "collection")
	data, err := ioutil.ReadFile(p)
	if err != nil {
		return nil, err
	}

	// TODO: implement newCollection function
	c := Collection{baseDir: path.Join(s.baseDir, name), s: s}

	dec := gob.NewDecoder(bytes.NewBuffer(data))
	err = dec.Decode(&c)
	if err != nil {
		return nil, err
	}

	//c.root = c.newPage()
	//c.root.ID = c.RootPage

	//err = c.loadPage(c.root)
	//if err != nil {
	//return nil, err
	//}

	fmt.Println("LOADED COLLECTION", c.Name)
	fmt.Println(c)

	return &c, err
}

// NewCollection returns a store
func NewCollection(t int) *Collection {
	c := &Collection{}

	//if c.root == nil {
		//c.root = c.newPage()
		//c.root.leaf = true
	//}

	return c
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
