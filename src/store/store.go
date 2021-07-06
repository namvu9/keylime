package store

import (
	"bytes"
	"encoding/gob"
	"fmt"
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

type CollectionFactory struct {
	storage ReadWriterTo
	repo    repository.Repository
}

func (cf CollectionFactory) New() types.Identifiable {
	nop := repository.NoOpFactory{}
	return nop.New()
}

func (cf CollectionFactory) Restore(item types.Identifiable) error {
	c, ok := item.(*collection)
	if !ok {
		return fmt.Errorf("CollectionFactory does not know how to handle item %v", item)
	}

	c.repo = repository.WithScope(cf.repo, c.ID())
	c.storage = cf.storage

	return c.load()
}

func newCollectionFactory(r repository.Repository, s ReadWriterTo) CollectionFactory {
	return CollectionFactory{
		repo: r,
		storage: s,
	}
}

func (s *Store) Collection(name string) (types.Collection, error) {
	repo := repository.WithScope(s.repo, name)

	item, err := repo.Get(name)

	if err != nil {
		if os.IsNotExist(err) {
			c := newCollection(name, s.storage, s.repo)
			err := repo.SaveCommit(c)
			if err != nil {
				return nil, err
			}
			return c, err
		}

		return nil, err
	}

	c, ok := item.(*collection)
	if !ok {
		return nil, fmt.Errorf("Could not load collection %s", name)
	}

	return c, nil
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

	s.repo = repository.WithFactory(s.repo, newCollectionFactory(s.repo, s.storage))

	return s
}
