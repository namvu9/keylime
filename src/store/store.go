package store

import (
	"bytes"
	"encoding/gob"
	"io"
	"os"

	"github.com/namvu9/keylime/src/repository"
	"github.com/namvu9/keylime/src/types"
)

type Store struct {
	baseDir     string
	t           int
	collections map[string]*collection
	storage     ReadWriterTo
	repo        repository.Repository
}

func (s Store) Collection(name string) types.Collection {
	c, ok := s.collections[name]
	if !ok {
		c := newCollection(name, s.storage, s.repo)
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

type GobCodec struct{}

func (gc GobCodec) Encode(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(v)
	if err != nil {
		return nil, err
	} 

	b := buf.Bytes()
	return b, nil
}

func (gc GobCodec) Decode(data []byte, dst interface{}) error {
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	err := dec.Decode(dst)
	if err != nil {
		return err
	}

	return nil
}

// New instantiates a store with the provided config and
// options
func New(cfg *Config, opts ...Option) *Store {
	s := &Store{
		baseDir:     cfg.BaseDir,
		t:           cfg.T,
		collections: make(map[string]*collection),
		repo:        repository.New(cfg.BaseDir, GobCodec{}, repository.NewFS(cfg.BaseDir)),
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
